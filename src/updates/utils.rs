use std::{
    io::{Read, Seek},
    path::Path,
};

use super::UResult;
use zip::ZipArchive;

pub fn extract_build<P: AsRef<Path>, R: Read + Seek>(path: P, stream: R) -> UResult<()> {
    let mut archive = ZipArchive::new(stream)?;
    archive.extract(&path)?;

    // chmod +x Robust.Server
    #[cfg(target_family = "unix")]
    {
        let server_path = path.as_ref().join("Robust.Server");
        if std::fs::exists(&server_path).unwrap_or(false) {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&server_path)?.permissions();
            perms.set_mode(perms.mode() | 0o111);
            std::fs::set_permissions(server_path, perms)?;
        }
    }

    Ok(())
}
