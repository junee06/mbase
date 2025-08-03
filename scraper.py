import os, json, time, requests
from tqdm import tqdm

RETRIES = 3
DELAY = 1
BATCH_SIZE = 10000
RETRY_LIMIT = 500
API_URL = "https://mdl-pi.vercel.app/id/"
PROGRESS_FILE = "progress.json"
FAILED_LOG = "failed_slugs.log"

def load_failed_slugs():
    if os.path.exists(FAILED_LOG):
        with open(FAILED_LOG, "r") as f:
            slugs = list(set(line.strip() for line in f if line.strip()))
        return slugs[:RETRY_LIMIT], slugs[RETRY_LIMIT:]
    return [], []

def write_failed_slugs(slugs):
    with open(FAILED_LOG, "w") as f:
        f.write("\n".join(slugs) + "\n")

def request_slug(slug):
    url = API_URL + slug
    for attempt in range(RETRIES):
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 200:
                return res.json()
            else:
                raise Exception(f"Status {res.status_code}")
        except Exception as e:
            if attempt < RETRIES - 1:
                time.sleep(DELAY)
            else:
                return None

# Load drama list and progress
with open("drama_ids.json") as f:
    drama_list = json.load(f)

if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE) as f:
        progress = json.load(f)
        current = progress.get("current", 0)
else:
    current = 0

# Load failed slugs
retry_slugs, remaining_failed = load_failed_slugs()

# Batch slice
total = len(drama_list)
end = min(current + BATCH_SIZE, total)
batch = drama_list[current:end]

# Build slug list (retries first)
slugs = retry_slugs + [item["url"].split("/")[-1] for item in batch]
results, failed = [], []

print(f"Retrying {len(retry_slugs)} failed slugs + {len(batch)} new slugs...")

# Scrape all slugs
os.makedirs("data", exist_ok=True)

for slug in tqdm(slugs, desc=f"Scraping {current + 1} to {end}"):
    data = request_slug(slug)
    if data:
        results.append(data)
    else:
        failed.append(slug)

# Save JSON output
filename = f"data/mdl_batch_{current//BATCH_SIZE + 1}.json"
with open(filename, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

# Save new failed slugs (retry leftovers + newly failed)
all_failed = remaining_failed + failed
if all_failed:
    write_failed_slugs(all_failed)
    print(f"{len(failed)} slugs failed this batch. Logged for retry.")
else:
    if os.path.exists(FAILED_LOG):
        os.remove(FAILED_LOG)
    print("All slugs successful. No failures.")

# Update progress (new entries only)
with open(PROGRESS_FILE, "w") as f:
    json.dump({"current": end}, f)

print(f"✅ Finished batch {current // BATCH_SIZE + 1} ({end - current} entries + {len(retry_slugs)} retries).")
