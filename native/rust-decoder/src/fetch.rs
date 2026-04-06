use reqwest::Client;
use tokio::runtime::Builder;
use tokio::task::JoinSet;

pub type OutputCallback = extern "C" fn(seq: u64, data_ptr: *const u8, data_len: u32);

pub const RANGE_DONE_SEQ: u64 = u64::MAX;

pub fn spawn_download_and_decode_range(
    archive_url: String,
    from: u64,
    to: u64,
    concurrency: u32,
    output_ptr: *mut u8,
    output_capacity: u32,
    callback: OutputCallback,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_addr = output_ptr as usize;
    let concurrency = concurrency.max(1);

    std::thread::Builder::new()
        .name("jun-rust-fetch-decode".to_string())
        .spawn(move || {
            let result = download_and_decode_range(
                &archive_url,
                from,
                to,
                concurrency,
                output_addr as *mut u8,
                output_capacity,
                callback,
            );

            if let Err(err) = result {
                eprintln!("[rust-fetch] range failed: {err}");
            }

            callback(RANGE_DONE_SEQ, std::ptr::null(), 0);
        })?;

    Ok(())
}

fn download_and_decode_range(
    archive_url: &str,
    from: u64,
    to: u64,
    concurrency: u32,
    output_ptr: *mut u8,
    output_capacity: u32,
    callback: OutputCallback,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let runtime = Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async move {
        let concurrency = concurrency.max(1) as usize;
        let base_url = archive_url.trim_end_matches('/').to_string();
        let client = Client::builder()
            .pool_max_idle_per_host(concurrency)
            .build()?;

        let mut join_set = JoinSet::new();
        let mut next_seq = from;

        for _ in 0..concurrency {
            if next_seq > to {
                break;
            }
            spawn_download(&mut join_set, client.clone(), base_url.clone(), next_seq);
            next_seq += 1;
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok((seq, compressed))) => {
                    let output = unsafe {
                        std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize)
                    };
                    match crate::decode_checkpoint_binary_inner(&compressed, output) {
                        Ok(bytes_written) => {
                            callback(seq, output.as_ptr(), bytes_written as u32);
                        }
                        Err(err) => {
                            eprintln!("[rust-fetch] decode failed for checkpoint {seq}: {err}");
                        }
                    }
                }
                Ok(Err(err)) => {
                    eprintln!("[rust-fetch] checkpoint failed: {err}");
                }
                Err(err) => {
                    eprintln!("[rust-fetch] task join failed: {err}");
                }
            }

            if next_seq <= to {
                spawn_download(&mut join_set, client.clone(), base_url.clone(), next_seq);
                next_seq += 1;
            }
        }

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    Ok(())
}

fn spawn_download(join_set: &mut JoinSet<Result<(u64, Vec<u8>), String>>, client: Client, base_url: String, seq: u64) {
    join_set.spawn(async move {
        let url = format!("{base_url}/{seq}.binpb.zst");
        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|err| format!("GET {url}: {err}"))?;

        if !response.status().is_success() {
            return Err(format!("GET {url}: HTTP {}", response.status()));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|err| format!("read body {url}: {err}"))?;

        Ok((seq, bytes.to_vec()))
    });
}
