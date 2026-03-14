## 2026-03-14 - ZIP Bomb Vulnerability in File Extraction
**Vulnerability:** File extraction using `std::io::copy` did not impose any limits on the decompressed file size, making it susceptible to decompression (ZIP) bombs.
**Learning:** External data should never be fully trusted. Archive contents can falsely report small uncompressed sizes while containing gigabytes of data. Using unbounded extraction methods without file size limits creates trivial denial of service (DoS) vulnerabilities by exhausting disk space.
**Prevention:** Always validate uncompressed sizes via metadata if possible, but crucially, enforce a hard limit on extracted bytes using bounded read methods like `std::io::Read::take(MAX_SIZE)` when writing to disk.
