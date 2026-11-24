//! Chunk compaction for fragmented files.

use juicefs_core::{CHUNK_SIZE, Slice};
use juicefs_meta::MetaError;

use crate::Vfs;
use crate::error::VfsError;

/// Compacts all fragmented chunks (`slices.len() > 1`) of the file at `path`.
///
/// Returns the number of chunks compacted into single contiguous slices.
pub(crate) async fn compact_file(vfs: &Vfs, path: &str) -> Result<usize, VfsError> {
    let (ino, attr) = vfs.resolve_path_follow(path).await?;
    if attr.is_dir() {
        return Err(MetaError::IsDir(path.to_owned()).into());
    }
    if !attr.is_file() {
        return Err(MetaError::InvalidInput(format!("not a regular file: {path}")).into());
    }
    if attr.length == 0 {
        return Ok(0);
    }

    let num_chunks = attr.length.div_ceil(CHUNK_SIZE) as u32;
    let mut compacted = 0usize;

    for indx in 0..num_chunks {
        let slices = vfs.meta().read_chunk(ino, indx).await?;
        if slices.len() <= 1 {
            continue;
        }
        let chunk_len: usize = slices.iter().map(|s| s.len as usize).sum();
        if chunk_len == 0 {
            continue;
        }

        let mut buf = vec![0u8; chunk_len];
        let read = vfs.read_from_slices(&slices, 0, &mut buf).await?;
        buf.truncate(read);

        let new_slice_id = vfs.meta().new_slice().await?;
        if let Err(err) = vfs.chunks().write_slice(new_slice_id, &buf).await {
            return Err(err.into());
        }

        let slice_len = buf.len() as u32;
        let compacted_slice = Slice {
            id: new_slice_id,
            size: slice_len,
            off: 0,
            len: slice_len,
        };
        if let Err(err) = vfs
            .meta()
            .write_chunk(ino, indx, 0, compacted_slice, attr.length)
            .await
        {
            let _ = vfs.chunks().remove_slice(new_slice_id, buf.len()).await;
            return Err(err.into());
        }
        compacted += 1;
    }

    Ok(compacted)
}
