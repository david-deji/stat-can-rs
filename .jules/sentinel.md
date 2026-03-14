## 2024-05-24 - Zip Bomb / DoS Vulnerability in File Extraction

**Vulnerability:** The code used unrestricted `std::io::copy` to extract files from zip archives, making it vulnerable to decompression bombs (Zip Bombs). An attacker could provide a small zip file that expands to terabytes, causing a Denial of Service (DoS) due to disk space exhaustion.

**Learning:** This vulnerability existed because zip extraction lacked reasonable size limits for decompressed files. `ZipArchive` naturally doesn't enforce extraction limits unless specified.

**Prevention:** To prevent this, always limit the amount of data read from decompressed streams. Using `std::io::Read::take(limit)` wraps the reader to ensure no more than the specified limit is extracted, and verifying if more data exists afterward properly flags malicious/oversized files.
