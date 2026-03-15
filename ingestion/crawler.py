"""
TopCV Job Crawler — fix 429 + fix pagination detect
"""
import requests
from bs4 import BeautifulSoup
import json
import time
import os
import random
from datetime import datetime

BASE_URL = "https://www.topcv.vn"
LIST_URL = "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.topcv.vn/",
}


# ─────────────────────────────────────────
# HTTP — xử lý 429 bằng backoff dài hơn
# ─────────────────────────────────────────
def get_soup(url: str, retries: int = 5) -> BeautifulSoup | None:
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)

            # 429 Too Many Requests → backoff dài rồi retry
            if resp.status_code == 429:
                wait = 10 * (attempt + 1) + random.uniform(2, 5)
                print(f"  [429] Rate limited — chờ {wait:.0f}s rồi retry ({attempt+1}/{retries})")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")

        except requests.RequestException as e:
            wait = 3 * (attempt + 1)
            print(f"  [Attempt {attempt+1}/{retries}] Error: {e} — chờ {wait}s")
            if attempt < retries - 1:
                time.sleep(wait)

    return None


# ─────────────────────────────────────────
# TITLE
# ─────────────────────────────────────────
def extract_title(title_tag) -> str:
    span = title_tag.select_one("span[data-original-title]")
    return (
        (span.get("data-original-title", "").strip() if span else "")
        or (span.get_text(strip=True) if span else "")
        or title_tag.get("title", "").strip()
        or title_tag.get("aria-label", "").strip()
        or title_tag.get_text(strip=True)
    )


# ─────────────────────────────────────────
# LIST PAGE
# ─────────────────────────────────────────
def extract_job_links_from_list_page(soup: BeautifulSoup) -> list[dict]:
    jobs = []
    for item in soup.select("div.job-item-search-result[data-job-id]"):
        job_id = item.get("data-job-id", "")

        title_tag = item.select_one("h3.title a[href]") or item.select_one("div.avatar a[href]")
        job_url, job_title = "", ""
        if title_tag:
            job_url   = title_tag.get("href", "").split("?")[0]
            job_title = extract_title(title_tag)

        company_tag  = item.select_one("a.company .company-name")
        salary_tag   = item.select_one("label.title-salary") or item.select_one("label.salary span")
        location_tag = item.select_one("label.address .city-text")
        exp_tag      = item.select_one("label.exp span")
        tag_elements = item.select("div.tag a.item-tag")

        jobs.append({
            "job_id":     job_id,
            "title":      job_title,
            "url":        job_url,
            "company":    company_tag.get_text(strip=True)  if company_tag  else "",
            "salary":     salary_tag.get_text(strip=True)   if salary_tag   else "",
            "location":   location_tag.get_text(strip=True) if location_tag else "",
            "experience": exp_tag.get_text(strip=True)      if exp_tag      else "",
            "tags":       [t.get_text(strip=True) for t in tag_elements],
        })
    return jobs


# ─────────────────────────────────────────
# DETAIL PAGE
# ─────────────────────────────────────────
def extract_job_detail(soup: BeautifulSoup) -> dict:
    result = {
        "tags_requirement":      [],
        "tags_specialization":   [],
        "mo_ta_cong_viec_html":  "",
        "yeu_cau_ung_vien_html": "",
        "quyen_loi_html":        "",
        "deadline":              "",
    }

    info_box = soup.select_one("#box-job-information-detail")
    if info_box:
        for group in info_box.select("div.job-tags__group"):
            name_el    = group.select_one("div.job-tags__group-name")
            group_name = name_el.get_text(strip=True) if name_el else ""
            items      = [a.get_text(strip=True) for a in group.select("a.item")]
            if "Yêu cầu" in group_name:
                result["tags_requirement"] = items
            elif "Chuyên môn" in group_name:
                result["tags_specialization"] = items

    for item in soup.select("div.job-description__item"):
        h3 = item.select_one("h3")
        if not h3:
            continue
        heading     = h3.get_text(strip=True)
        content_div = item.select_one("div.job-description__item--content")
        if not content_div:
            continue
        raw_html = str(content_div)
        if "Mô tả công việc" in heading:
            result["mo_ta_cong_viec_html"] = raw_html
        elif "Yêu cầu ứng viên" in heading:
            result["yeu_cau_ung_vien_html"] = raw_html
        elif "Quyền lợi" in heading:
            result["quyen_loi_html"] = raw_html

    dl = soup.select_one("div.job-detail__information-detail--actions-label")
    if dl:
        result["deadline"] = dl.get_text(strip=True)

    return result


# ─────────────────────────────────────────
# LƯU PER-PAGE
# ─────────────────────────────────────────
def save_page_json(jobs: list[dict], page: int, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"page_{page:03d}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    return path


# ─────────────────────────────────────────
# MAIN CRAWL
# ─────────────────────────────────────────
def crawl(
    max_pages:    int   = 60,
    delay:        float = 3.0,
    fetch_detail: bool  = True,
    output_dir:   str   = "topcv_pages",
    merged_path:  str   = "topcv_jobs.json",
) -> list[dict]:
    """
    Crawl max_pages trang, lưu mỗi trang thành file JSON riêng ngay lập tức.
    Dừng khi: trang trống | đạt max_pages.
    KHÔNG dùng get_total_pages() vì TopCV render pagination bằng JS.
    """
    run_id   = datetime.now().strftime("%Y%m%d_%H%M%S")
    page_dir = os.path.join(output_dir, run_id)
    all_jobs: list[dict] = []

    print(f"Output dir : {page_dir}")
    print(f"Merged file: {merged_path}")
    print(f"Max pages  : {max_pages}\n")

    for page in range(1, max_pages + 1):
        page_url = LIST_URL if page == 1 else f"{LIST_URL}?page={page}"
        print(f"[Page {page}/{max_pages}] {page_url}")

        soup = get_soup(page_url)
        if soup is None:
            print("  → Fetch thất bại, dừng.")
            break

        jobs = extract_job_links_from_list_page(soup)
        print(f"  → Tìm thấy {len(jobs)} jobs")

        # Trang trống = đã hết kết quả
        if not jobs:
            print("  → Trang trống, dừng.")
            break

        # Fetch detail từng job
        if fetch_detail:
            for idx, job in enumerate(jobs, 1):
                if not job["url"]:
                    continue
                print(f"     [{idx}/{len(jobs)}] {job['url']}")
                detail_soup = get_soup(job["url"])
                if detail_soup:
                    job.update(extract_job_detail(detail_soup))
                # Delay ngẫu nhiên giữa các detail request để tránh 429
                time.sleep(delay + random.uniform(0, 1.5))

        # Lưu ngay trang này
        saved = save_page_json(jobs, page, page_dir)
        all_jobs.extend(jobs)
        print(f"  → Saved: {saved} | Tổng tích lũy: {len(all_jobs)} jobs")

        # Delay giữa các trang (dài hơn để tránh 429 trên list page)
        if page < max_pages:
            wait = delay * 2 + random.uniform(1, 3)
            print(f"  → Chờ {wait:.1f}s trước trang tiếp...\n")
            time.sleep(wait)

    # Gộp tất cả thành một file cuối
    if all_jobs:
        with open(merged_path, "w", encoding="utf-8") as f:
            json.dump(all_jobs, f, ensure_ascii=False, indent=2)
        print(f"\n✓ Merged {len(all_jobs)} jobs → {merged_path}")

    print(f"✓ Per-page files → {page_dir}/")
    return all_jobs


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────
if __name__ == "__main__":
    jobs = crawl(
        max_pages=60,
        delay=3.0,          
        fetch_detail=True,
        output_dir="docker/storage/data_lake/bronze/topcv",
        merged_path="docker/storage/data_lake/bronze/topcv/topcv_jobs.json",
    )

    print(f"\nTổng cộng: {len(jobs)} jobs")
    if jobs:
        sample = jobs[0].copy()
        for key in ("mo_ta_cong_viec_html", "yeu_cau_ung_vien_html", "quyen_loi_html"):
            if sample.get(key):
                sample[key] = sample[key][:200] + "..."
        print("\n── Sample ──")
        print(json.dumps(sample, ensure_ascii=False, indent=2))