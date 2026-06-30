//! Complex end-to-end test: build a real `.tar.zst`, serve it, then run
//!
//!   pcurl URL | zstd -d | tar -x
//!
//! and verify the extracted tree is byte-identical to the original. Also does
//! a direct `-o file` download and compares it to the source archive.

mod common;

use std::path::Path;
use std::process::{Command, Stdio};

use common::{Mode, TestServer};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

const BIN: &str = env!("CARGO_BIN_EXE_pcurl");

fn have(tool: &str) -> bool {
    Command::new(tool)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Build a directory tree with nested folders and files of varied sizes/kinds.
fn build_source_tree(root: &Path) {
    let mut rng = StdRng::seed_from_u64(0xC0FFEE);
    std::fs::create_dir_all(root.join("a/b/c")).unwrap();
    std::fs::create_dir_all(root.join("a/d")).unwrap();
    std::fs::create_dir_all(root.join("e")).unwrap();

    // A handful of binary blobs of differing sizes (incompressible).
    let sizes = [
        1,
        1024,
        64 * 1024 + 7,
        1024 * 1024 + 3,
        4 * 1024 * 1024 + 123,
        8 * 1024 * 1024,
    ];
    for (i, &sz) in sizes.iter().enumerate() {
        let mut buf = vec![0u8; sz];
        rng.fill_bytes(&mut buf);
        let sub = ["a", "a/b", "a/b/c", "a/d", "e", "."][i % 6];
        std::fs::write(root.join(sub).join(format!("blob_{i}.bin")), &buf).unwrap();
    }

    // Some compressible text files so zstd has real work to do.
    for i in 0..5 {
        let reps = rng.gen_range(1000..5000);
        let line = format!("line {i}: the quick brown fox jumps over the lazy dog\n");
        let text = line.repeat(reps);
        std::fs::write(root.join("e").join(format!("text_{i}.txt")), text).unwrap();
    }
}

/// Create `archive.tar.zst` from `src` and return its bytes.
fn make_tar_zst(src: &Path, work: &Path) -> Vec<u8> {
    let tar_path = work.join("archive.tar");
    let zst_path = work.join("archive.tar.zst");

    // Deterministic-ish archive: sort entries for stable ordering.
    let status = Command::new("tar")
        .arg("--sort=name")
        .arg("-C")
        .arg(src)
        .arg("-cf")
        .arg(&tar_path)
        .arg(".")
        .status()
        .expect("run tar");
    assert!(status.success(), "tar failed");

    let status = Command::new("zstd")
        .arg("-3")
        .arg("-q")
        .arg("-f")
        .arg(&tar_path)
        .arg("-o")
        .arg(&zst_path)
        .status()
        .expect("run zstd");
    assert!(status.success(), "zstd failed");

    std::fs::read(&zst_path).unwrap()
}

#[test]
fn download_pipe_through_zstd_tar() {
    if !have("tar") || !have("zstd") {
        eprintln!("skipping: tar/zstd not available");
        return;
    }

    let work = tempfile::tempdir().unwrap();
    let src = work.path().join("src");
    build_source_tree(&src);
    let archive = make_tar_zst(&src, work.path());
    eprintln!("archive.tar.zst is {} bytes", archive.len());

    let server = TestServer::start(archive.clone(), Mode::Range);

    // 1) Direct download to a file must reproduce the archive byte-for-byte.
    let dl_path = work.path().join("downloaded.tar.zst");
    let status = Command::new(BIN)
        .arg(&server.url)
        .args(["-c", "8", "-s", "1M", "-q", "-t", "5", "-o"])
        .arg(&dl_path)
        .status()
        .expect("spawn pcurl");
    assert!(status.success(), "direct download failed");
    let downloaded = std::fs::read(&dl_path).unwrap();
    assert_eq!(
        downloaded.len(),
        archive.len(),
        "downloaded archive size mismatch"
    );
    assert!(downloaded == archive, "downloaded archive bytes differ");

    // 2) Full pipeline: pcurl URL | zstd -d | tar -x into a fresh dir.
    let outdir = work.path().join("extracted");
    std::fs::create_dir_all(&outdir).unwrap();
    let script = format!(
        "set -o pipefail; '{}' '{}' -c 8 -s 1M -q -t 5 | zstd -d | tar -x -C '{}'",
        BIN,
        server.url,
        outdir.display()
    );
    let status = Command::new("bash")
        .arg("-c")
        .arg(&script)
        .status()
        .expect("spawn pipeline");
    assert!(status.success(), "download|zstd|tar pipeline failed");

    // 3) The extracted tree must match the source exactly.
    let diff = Command::new("diff")
        .arg("-r")
        .arg(&src)
        .arg(&outdir)
        .output()
        .expect("run diff");
    assert!(
        diff.status.success(),
        "extracted tree differs from source:\n{}\n{}",
        String::from_utf8_lossy(&diff.stdout),
        String::from_utf8_lossy(&diff.stderr),
    );
}
