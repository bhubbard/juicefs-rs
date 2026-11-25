//! Platform-specific FUSE unmount helpers.

use std::path::Path;
use std::process::Command;

use crate::error::FuseError;

/// Unmounts a mounted FUSE filesystem at `mountpoint`.
///
/// On macOS this invokes `umount <mountpoint>`. On Linux it tries
/// `fusermount -u <mountpoint>` first and falls back to `umount <mountpoint>`.
pub fn unmount(mountpoint: &Path) -> Result<(), FuseError> {
    if mountpoint.as_os_str().is_empty() {
        return Err(FuseError::invalid("mountpoint must not be empty"));
    }

    #[cfg(target_os = "macos")]
    {
        run_unmount_cmd("umount", &[mountpoint.as_os_str()])
    }

    #[cfg(target_os = "linux")]
    {
        let u_flag = std::ffi::OsStr::new("-u");
        let mp = mountpoint.as_os_str();
        match run_unmount_cmd("fusermount", &[u_flag, mp]) {
            Ok(()) => Ok(()),
            Err(FuseError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
                run_unmount_cmd("umount", &[mp])
            }
            Err(first_err) => run_unmount_cmd("umount", &[mp]).map_err(|_| first_err),
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = mountpoint;
        Err(FuseError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "unmount is only implemented for macOS and Linux",
        )))
    }
}

fn run_unmount_cmd(program: &str, args: &[&std::ffi::OsStr]) -> Result<(), FuseError> {
    let output = Command::new(program).args(args).output()?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    let msg = if stderr.is_empty() {
        format!("{program} exited with status {}", output.status)
    } else {
        format!("{program} failed: {stderr}")
    };
    Err(FuseError::Io(std::io::Error::other(msg)))
}
