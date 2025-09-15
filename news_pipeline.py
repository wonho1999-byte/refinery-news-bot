# -*- coding: utf-8 -*-
"""
news_pipeline.py  (patched: robust fetch/logging/preview/SMTP logs)
- Google News(RSS)에서 전일 lookback_hours 이내 기사 수집
- 대분류/중분류/키워드 별 검색 → 정유 연관성 스코어 → 정치/잡음 필터 → 중복 제거
- OpenAI로 1~2문장 요약 (enable_summarize=true일 때)
- 카드형 HTML 이메일 전송 (섹션별 최대 N건)
- email_preview.html을 항상 저장(워크플로 아티팩트/로컬 확인용)
"""

import os
import sys
import ssl
import smtplib
import pytz
import yaml
import feedparser
import requests

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.parse import quote_plus

from utils.scoring import compute_score, apply_unrelated_penalty
from utils.dedupe import dedupe_items
from utils.summarize import summarize_1_2

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
except Exception:
    BlockingScheduler = None


# ---------- Config/Helpers ----------

def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def google_news_rss(query, cfg):
    """
    Google News RSS를 requests + 명시적 UA로 가져온 후 feedparser로 파싱
    (일부 환경에서 빈 피드 방지)
    """
    base = cfg["sources"]["google_news"]["base"]
    params = {
        "q": f'{query} when:1d',
        "hl": cfg["sources"]["google_news"]["hl"],
        "gl": cfg["sources"]["google_news"]["gl"],
        "ceid": cfg["sources"]["google_news"]["ceid"],
    }
    q = "&".join([f"{k}={quote_plus(v)}" for k, v in params.items()])
    url = f"{base}?{q}"
    headers = {"User-Agent": "Mozilla/5.0 (refinery-news-bot; +https://github.com)"}
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    return feedparser.parse(r.text)

def extract_text(entry):
    # title + summary에서 텍스트 추출
    title = entry.get("title", "")
    summary_html = entry.get("summary", "")
    try:
        summary = BeautifulSoup(summary_html, "html5lib").get_text(" ", strip=True)
    except Exception:
        summary = BeautifulSoup(summary_html, "html.parser").get_text(" ", strip=True)
    return title, summary

def is_block_domain(link, cfg):
    for d in cfg["filters"]["block_domains"]:
        if d in link:
            return True
    return False

def within_window(published, tz, start_dt, end_dt):
    # feedparser의 published_parsed(tuple) → aware datetime으로 변환해 윈도우 체크
    if not published:
        return True  # 일부 소스는 시간 미제공 → 포함(과도 제외는 스코어/키워드로 걸러짐)
    try:
        dt = datetime(*published[:6], tzinfo=pytz.utc).astimezone(tz)
        return start_dt <= dt <= end_dt
    except Exception:
        return True


# ---------- Scoring/Filtering ----------

def build_query_terms(taxonomy_item):
    # 하나의 중분류에 대해 모든 키워드를 개별 호출(AND/OR 복잡성 회피)
    return taxonomy_item["keywords"]

def score_and_filter(items, hit_keywords, cfg):
    scored = []
    for it in items:
        text = f'{it["title"]} {it["summary"]}'
        s = compute_score(text, cfg)
        s += apply_unrelated_penalty(hit_keywords, text, cfg)
        if s < 0:
            continue  # 정치/연성 과도 페널티로 컷
        it["score"] = s
        scored.append(it)
    return sorted(scored, key=lambda x: x["score"], reverse=True)


# ---------- HTML/Email ----------

def make_html_email(grouped, cfg, start_dt, end_dt):
    head = f"""
    <html><body style="font-family:Arial,Helvetica,sans-serif;">
      <h2>정유 뉴스 요약 ({start_dt.strftime('%Y-%m-%d %H:%M')} ~ {end_dt.strftime('%Y-%m-%d %H:%M')} KST)</h2>
      <p style="color:#666;">정유사 직원 관점 유의미 기사만 선별 · 요약했습니다.</p>
    """
    cards = []
    for major, minors in grouped.items():
        cards.append(f'<h3 style="border-bottom:2px solid #eee;padding-bottom:4px;">{major}</h3>')
        for minor, items in minors.items():
            if not items:
                continue
            cards.append(f'<h4 style="margin:10px 0 6px 0;color:#0a4;">{minor}</h4>')
            for it in items:
                cards.append(f"""
                <div style="border:1px solid #eee;border-radius:10px;padding:12px;margin:8px 0;">
                  <div style="font-weight:600;margin-bottom:6px;">{it['title']}</div>
                  <div style="color:#333;margin-bottom:8px;">{it['summary_short']}</div>
                  <a style="display:inline-block;background:#1565C0;color:#fff;padding:8px 12px;border-radius:6px;text-decoration:none;"
                     href="{it['link']}" target="_blank" rel="noopener">원문 보기</a>
                </div>
                """)
    tail = "</body></html>"
    return head + "\n".join(cards) + tail

def send_email(html, cfg):
    print(f"[SMTP] host={cfg['email']['smtp_host']} port={cfg['email']['smtp_port']} tls={cfg['email']['use_tls']}")
    msg = MIMEMultipart("alternative")
    subject = f"{cfg['email']['subject_prefix']} {datetime.now().strftime('%Y-%m-%d')}"
    msg["Subject"] = subject
    msg["From"] = f"{cfg['email']['from_name']} <{cfg['email']['from_addr']}>"
    msg["To"] = ", ".join(cfg["email"]["to_addrs"])
    msg.attach(MIMEText(html, "html", "utf-8"))

    server = smtplib.SMTP(cfg["email"]["smtp_host"], cfg["email"]["smtp_port"], timeout=20)
    if cfg["email"]["use_tls"]:
        server.starttls()
    pwd = os.getenv(cfg["email"]["password_env"], "")
    if cfg["email"]["username"]:
        print(f"[SMTP] login as {cfg['email']['username']} (pwd={'SET' if bool(pwd) else 'EMPTY'})")
        server.login(cfg["email"]["username"], pwd)
    server.sendmail(cfg["email"]["from_addr"], cfg["email"]["to_addrs"], msg.as_string())
    server.quit()


# ---------- Main Run ----------

def run_once():
    cfg = load_config()
    tz = pytz.timezone(cfg["app"]["timezone"])
    now = datetime.now(tz)
    end_dt = now
    start_dt = end_dt - timedelta(hours=cfg["app"]["lookback_hours"])

    print(f"[INFO] Window: {start_dt} ~ {end_dt} {cfg['app']['timezone']}")
    taxonomy = cfg["taxonomy"]
    grouped = {}
    total_raw = 0
    total_kept = 0

    # 수집
    for tax in taxonomy:
        major, minor = tax["major"], tax["minor"]
        grouped.setdefault(major, {})
        grouped[major].setdefault(minor, [])
        keywords = build_query_terms(tax)

        bucket = []
        for kw in keywords:
            try:
                d = google_news_rss(kw, cfg)
                got = len(d.entries)
                print(f"[FETCH] {major}/{minor}/{kw}: entries={got}")
                items = []
                for e in d.entries:
                    link = e.get("link", "")
                    if not link or is_block_domain(link, cfg):
                        continue
                    title, summary = extract_text(e)
                    if not title:
                        continue
                    if not within_window(getattr(e, "published_parsed", None), tz, start_dt, end_dt):
                        continue
                    items.append({
                        "title": title,
                        "summary": summary,
                        "link": link,
                        "published": getattr(e, "published", ""),
                        "source": getattr(e, "source", {}).get("title") if hasattr(e, "source") else "",
                    })
                total_raw += len(items)
                bucket.extend(items)
            except Exception as ex:
                print(f"[WARN] fetch failed for {kw}: {ex}")

        before = len(bucket)
        bucket = dedupe_items(bucket)
        after_dedupe = len(bucket)

        bucket = score_and_filter(bucket, set(keywords), cfg)
        after_score = len(bucket)
        kept = bucket[:cfg["app"]["max_items_per_subcategory"]]
        total_kept += len(kept)
        grouped[major][minor] = kept
        print(f"[KEEP] {major}/{minor}: before={before}, deduped={after_dedupe}, scored={after_score}, kept={len(kept)}")

    # 요약
    for major in grouped:
        for minor in grouped[major]:
            for it in grouped[major][minor]:
                text = f"{it['title']}. {it['summary']}"
                it["summary_short"] = summarize_1_2(text, cfg)

    # HTML 생성
    html = make_html_email(grouped, cfg, start_dt, end_dt)

    # 미리보기 저장 (항상)
    try:
        with open("email_preview.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("[INFO] Saved email_preview.html")
    except Exception as ex:
        print(f"[WARN] preview save failed: {ex}")

    print(f"[SUMMARY] total_raw={total_raw}, total_kept={total_kept}")
    if total_kept == 0:
        print("[INFO] No items kept; sending email anyway (empty) to validate SMTP...")

    # 메일 전송
    try:
        send_email(html, cfg)
        print(f"[OK] Sent {total_kept} items.")
    except Exception as ex:
        print(f"[ERROR] send_email failed: {ex}")
        raise


def main():
    # --once: 즉시 한 번 실행 (GitHub Actions 권장)
    if "--once" in sys.argv or BlockingScheduler is None:
        run_once()
        return

    # 스케줄러 모드(로컬/서버에서 상시 구동 시)
    cfg = load_config()
    tz = pytz.timezone(cfg["app"]["timezone"])
    sched = BlockingScheduler(timezone=tz)
    sched.add_job(run_once, 'cron', hour=cfg["app"]["run_time_hour"], minute=0)
    print("[Scheduler] Started. Will run daily at %02d:00 %s." % (cfg["app"]["run_time_hour"], cfg["app"]["timezone"]))
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()
