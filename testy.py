import requests

def test_url_file_size(url, max_size_mb=50):
    headers = {"User-Agent": "MyApp/2.0 (myemail@example.com)"}
    
    print(f"Testing URL: {url}")
    
    # Step 1: Use HEAD request to check Content-Length
    try:
        response = requests.head(url, headers=headers)
        response.raise_for_status()
        
        # Check Content-Length header
        content_length = response.headers.get("Content-Length")
        if content_length:
            file_size_mb = int(content_length) / (1024 * 1024)
            print(f"Content-Length: {file_size_mb:.2f} MB")
            
            if file_size_mb > max_size_mb:
                print(f"File too large (>{max_size_mb} MB). Skipping download.")
                return
        else:
            print("Content-Length header is missing.")

    except requests.RequestException as e:
        print(f"HEAD request failed: {e}")
        return

    # Step 2: Fetch the file incrementally and check size dynamically
    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        max_size_bytes = max_size_mb * 1024 * 1024
        downloaded = 0

        for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
            downloaded += len(chunk)
            print(f"Downloaded: {downloaded / (1024 * 1024):.2f} MB")
            
            if downloaded > max_size_bytes:
                print(f"File exceeds size limit ({max_size_mb} MB). Stopping download.")
                response.close()
                return

        print("File downloaded successfully within size limits.")
    except requests.RequestException as e:
        print(f"GET request failed: {e}")

# Test the script with your specific URL
test_url = "https://www.sec.gov/Archives/edgar/data/1111565/0001775697-24-000667.txt"
test_url_file_size(test_url, max_size_mb=50)  # Set your size limit (in MB)

#test_url = "https://www.sec.gov/Archives/edgar/data/1709447/0001145549-24-055127.txt"
