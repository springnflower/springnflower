import os
import sqlite3
from datetime import datetime
from functools import wraps
from io import BytesIO
from urllib.parse import urlencode

import pandas as pd
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

APP_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(APP_DIR, "data", "influencers.db")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
COLUMN_LABELS = {
    "influencer_id": "인플루언서ID",
    "platform": "플랫폼",
    "category_main": "카테고리(대)",
    "category_sub": "카테고리(소)",
    "account_name": "이름/계정명",
    "profile_url": "프로필URL",
    "instagram_username": "인스타그램아이디",
    "contact_email": "컨택이메일",
    "agency": "에이전시/소속",
    "followers_raw": "팔로워/구독자(원본)",
    "followers_num": "팔로워/구독자(숫자)",
    "follower_range": "팔로워 구간",
    "video_usage": "영상 활용도(高/中/低)",
    "target_2030_score": "2030 타깃 적합도(1~5)",
    "price_bdc": "단가_BDC",
    "price_ppl": "단가_PPL",
    "price_short": "단가_Short/Shorts",
    "price_ig": "단가_IG",
    "thumbnail_url": "썸네일",
    "dm_message": "DM문구",
    "notes": "비고",
}
DEFAULT_DISCOVER_LIMIT = 10


def get_db():
    if DATABASE_URL:
        db_url = DATABASE_URL
        if "sslmode=" not in db_url:
            joiner = "&" if "?" in db_url else "?"
            db_url = f"{db_url}{joiner}sslmode=require"
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        return conn, True
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn, False


def format_query(query, is_postgres):
    if not is_postgres:
        return query
    return query.replace("?", "%s")


def run_query(conn, is_postgres, query, params=None, fetch_one=False, fetch_all=False):
    params = params or ()
    q = format_query(query, is_postgres)
    cursor = conn.cursor()
    cursor.execute(q, params)
    if fetch_one:
        return cursor.fetchone()
    if fetch_all:
        return cursor.fetchall()
    return None


def init_db():
    conn, is_postgres = get_db()
    if is_postgres:
        conn.cursor().execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            )
            """
        )
        conn.cursor().execute(
            """
            CREATE TABLE IF NOT EXISTS influencers (
                id SERIAL PRIMARY KEY,
                influencer_id TEXT,
                platform TEXT,
                category_main TEXT,
                category_sub TEXT,
                account_name TEXT NOT NULL,
                profile_url TEXT,
                instagram_username TEXT,
                contact_email TEXT,
                agency TEXT,
                followers_raw TEXT,
                followers_num INTEGER,
                follower_range TEXT,
                video_usage TEXT,
                target_2030_score INTEGER,
                price_bdc TEXT,
                price_ppl TEXT,
                price_short TEXT,
                price_ig TEXT,
                thumbnail_url TEXT,
                dm_message TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        existing_cols = {
            row["column_name"]
            for row in run_query(
                conn,
                is_postgres,
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'influencers'
                """,
                fetch_all=True,
            )
        }
    else:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS influencers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                influencer_id TEXT,
                platform TEXT,
                category_main TEXT,
                category_sub TEXT,
                account_name TEXT NOT NULL,
                profile_url TEXT,
                instagram_username TEXT,
                contact_email TEXT,
                agency TEXT,
                followers_raw TEXT,
                followers_num INTEGER,
                follower_range TEXT,
                video_usage TEXT,
                target_2030_score INTEGER,
                price_bdc TEXT,
                price_ppl TEXT,
                price_short TEXT,
                price_ig TEXT,
                thumbnail_url TEXT,
                dm_message TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        existing_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(influencers)").fetchall()
        }
    required_cols = {
        "influencer_id": "TEXT",
        "platform": "TEXT",
        "category_main": "TEXT",
        "category_sub": "TEXT",
        "account_name": "TEXT",
        "profile_url": "TEXT",
        "instagram_username": "TEXT",
        "contact_email": "TEXT",
        "agency": "TEXT",
        "followers_raw": "TEXT",
        "followers_num": "INTEGER",
        "follower_range": "TEXT",
        "video_usage": "TEXT",
        "target_2030_score": "INTEGER",
        "price_bdc": "TEXT",
        "price_ppl": "TEXT",
        "price_short": "TEXT",
        "price_ig": "TEXT",
        "thumbnail_url": "TEXT",
        "dm_message": "TEXT",
        "notes": "TEXT",
        "created_at": "TEXT",
        "updated_at": "TEXT",
    }
    for col, col_type in required_cols.items():
        if col not in existing_cols:
            run_query(
                conn,
                is_postgres,
                f"ALTER TABLE influencers ADD COLUMN {col} {col_type}",
            )
    user = run_query(
        conn,
        is_postgres,
        "SELECT id FROM users WHERE username = ?",
        ("spler",),
        fetch_one=True,
    )
    if not user:
        run_query(
            conn,
            is_postgres,
            "INSERT INTO users (username, password_hash) VALUES (?, ?)",
            ("spler", generate_password_hash("spler123")),
        )
    conn.commit()
    conn.close()


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "dev-secret")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    init_db()

    def login_required(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if "user_id" not in session:
                return redirect(url_for("login"))
            return view(*args, **kwargs)

        return wrapped

    def coerce_int(value):
        try:
            if value is None or value == "":
                return None
            try:
                if pd.isna(value):
                    return None
            except Exception:
                pass
            if isinstance(value, str):
                cleaned = value.replace(",", "").strip()
                if cleaned == "":
                    return None
                return int(float(cleaned))
            return int(value)
        except (ValueError, TypeError):
            return None

    def clean_text(value):
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        return str(value).strip()

    def extract_youtube_thumbnail(url):
        if not url:
            return ""
        patterns = [
            ("watch?v=", "&"),
            ("youtu.be/", "?"),
            ("/shorts/", "?"),
        ]
        for marker, ender in patterns:
            if marker in url:
                video_id = url.split(marker, 1)[1].split(ender, 1)[0]
                if video_id:
                    return f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"
        return ""

    def fetch_thumbnail_url(url):
        if not url:
            return ""
        youtube_thumb = extract_youtube_thumbnail(url)
        if youtube_thumb:
            return youtube_thumb
        try:
            response = requests.get(
                url,
                timeout=5,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if response.status_code != 200:
                return ""
            soup = BeautifulSoup(response.text, "html.parser")
            og_image = soup.find("meta", property="og:image")
            if og_image and og_image.get("content"):
                return og_image["content"].strip()
        except Exception:
            return ""
        return ""

    def extract_instagram_username(url):
        if not url:
            return ""
        if "instagram.com" not in url:
            return ""
        cleaned = url.split("instagram.com/", 1)[1]
        cleaned = cleaned.split("?", 1)[0].strip("/")
        if not cleaned:
            return ""
        first = cleaned.split("/", 1)[0]
        if first in {"p", "reel", "tv", "stories", "explore"}:
            return ""
        return first

    def search_youtube_channels(query, limit):
        api_key = os.environ.get("YOUTUBE_API_KEY", "").strip()
        if not api_key:
            return {
                "error": "YouTube API 키(YOUTUBE_API_KEY)가 필요합니다.",
                "items": [],
            }
        params = {
            "part": "snippet",
            "type": "channel",
            "q": query,
            "maxResults": limit,
            "key": api_key,
        }
        url = f"https://www.googleapis.com/youtube/v3/search?{urlencode(params)}"
        try:
            response = requests.get(url, timeout=8)
            response.raise_for_status()
            data = response.json()
        except Exception:
            return {
                "error": "YouTube 검색에 실패했습니다. API 키/쿼리를 확인해 주세요.",
                "items": [],
            }

        channel_ids = [
            item["snippet"]["channelId"]
            for item in data.get("items", [])
            if item.get("id", {}).get("kind") == "youtube#channel"
        ]
        stats_map = {}
        if channel_ids:
            stats_params = {
                "part": "statistics",
                "id": ",".join(channel_ids),
                "key": api_key,
            }
            stats_url = f"https://www.googleapis.com/youtube/v3/channels?{urlencode(stats_params)}"
            try:
                stats_response = requests.get(stats_url, timeout=8)
                stats_response.raise_for_status()
                stats_data = stats_response.json()
                for item in stats_data.get("items", []):
                    stats_map[item["id"]] = item.get("statistics", {})
            except Exception:
                stats_map = {}

        items = []
        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            channel_id = snippet.get("channelId")
            stats = stats_map.get(channel_id, {})
            items.append(
                {
                    "platform": "YouTube",
                    "name": snippet.get("channelTitle", ""),
                    "url": f"https://www.youtube.com/channel/{channel_id}",
                    "thumbnail": snippet.get("thumbnails", {}).get("default", {}).get("url"),
                    "description": snippet.get("description", ""),
                    "followers": stats.get("subscriberCount"),
                }
            )
        return {"error": "", "items": items}

    def search_instagram_profiles(query, limit):
        serpapi_key = os.environ.get("SERPAPI_KEY", "").strip()
        if not serpapi_key:
            return {
                "error": "Instagram 검색은 SERPAPI_KEY 설정이 필요합니다.",
                "items": [],
            }
        params = {
            "engine": "google",
            "q": f"site:instagram.com {query}",
            "num": limit,
            "api_key": serpapi_key,
        }
        url = f"https://serpapi.com/search.json?{urlencode(params)}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception:
            return {
                "error": "Instagram 검색에 실패했습니다. SERPAPI_KEY를 확인해 주세요.",
                "items": [],
            }

        items = []
        for result in data.get("organic_results", []):
            link = result.get("link", "")
            username = extract_instagram_username(link)
            if not username:
                continue
            items.append(
                {
                    "platform": "Instagram",
                    "name": result.get("title", ""),
                    "url": link,
                    "thumbnail": result.get("thumbnail"),
                    "description": result.get("snippet", ""),
                    "followers": None,
                    "instagram_username": username,
                }
            )
        return {"error": "", "items": items}

    def normalize_columns(df):
        mapping = {
            "인플루언서ID": "influencer_id",
            "플랫폼": "platform",
            "카테고리(대)": "category_main",
            "카테고리(소)": "category_sub",
            "이름/계정명": "account_name",
            "프로필URL": "profile_url",
            "인스타그램아이디": "instagram_username",
            "컨택이메일": "contact_email",
            "에이전시/소속": "agency",
            "팔로워/구독자(원본)": "followers_raw",
            "팔로워/구독자(숫자)": "followers_num",
            "팔로워 구간": "follower_range",
            "영상 활용도(高/中/低)": "video_usage",
            "2030 타깃 적합도(1~5)": "target_2030_score",
            "단가_BDC": "price_bdc",
            "단가_PPL": "price_ppl",
            "단가_Short/Shorts": "price_short",
            "단가_IG": "price_ig",
            "썸네일": "thumbnail_url",
            "DM문구": "dm_message",
            "비고": "notes",
            "계정명": "account_name",
            "카테고리": "category_main",
            "주요 콘텐츠": "category_sub",
            "감성 키워드": "notes",
        }
        normalized = {}
        for col in df.columns:
            normalized[col] = mapping.get(col, col)
        return df.rename(columns=normalized)

    def build_filters_from_values(
        q,
        platform,
        category_main,
        category_sub,
        columns_param,
    ):
        q = (q or "").strip()
        platform = (platform or "").strip()
        category_main = (category_main or "").strip()
        category_sub = (category_sub or "").strip()
        columns_param = (columns_param or "").strip()

        where = []
        params = []
        if q:
            like = f"%{q}%"
            where.append(
                "("
                "influencer_id LIKE ? OR account_name LIKE ? OR platform LIKE ? OR "
                "category_main LIKE ? OR category_sub LIKE ? OR profile_url LIKE ? OR "
                "instagram_username LIKE ? OR contact_email LIKE ? OR agency LIKE ? OR "
                "follower_range LIKE ? OR dm_message LIKE ? OR notes LIKE ?"
                ")"
            )
            params.extend([like] * 12)
        if platform:
            where.append("platform = ?")
            params.append(platform)
        if category_main:
            where.append("category_main = ?")
            params.append(category_main)
        if category_sub:
            where.append("category_sub = ?")
            params.append(category_sub)

        where_clause = f"WHERE {' AND '.join(where)}" if where else ""
        all_columns = list(COLUMN_LABELS.keys())
        if columns_param:
            selected_columns = [
                col for col in columns_param.split(",") if col in COLUMN_LABELS
            ]
        else:
            selected_columns = all_columns

        return {
            "q": q,
            "platform": platform,
            "category_main": category_main,
            "category_sub": category_sub,
            "where_clause": where_clause,
            "params": params,
            "all_columns": all_columns,
            "selected_columns": selected_columns,
        }

    def build_filters(req):
        return build_filters_from_values(
            req.args.get("q", ""),
            req.args.get("platform", ""),
            req.args.get("category_main", ""),
            req.args.get("category_sub", ""),
            req.args.get("columns", ""),
        )

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "").strip()
            if not username or not password:
                flash("아이디와 비밀번호를 입력해 주세요.")
                return render_template("login.html")
            conn, is_postgres = get_db()
            user = run_query(
                conn,
                is_postgres,
                "SELECT id, password_hash FROM users WHERE username = ?",
                (username,),
                fetch_one=True,
            )
            conn.close()
            if not user or not check_password_hash(user["password_hash"], password):
                flash("로그인 정보가 올바르지 않습니다.")
                return render_template("login.html")
            session["user_id"] = user["id"]
            return redirect(url_for("index"))
        return render_template("login.html")

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.route("/")
    @login_required
    def index():
        filters = build_filters(request)
        conn, is_postgres = get_db()
        influencers = run_query(
            conn,
            is_postgres,
            f"""
            SELECT * FROM influencers
            {filters["where_clause"]}
            ORDER BY updated_at DESC
            """,
            filters["params"],
            fetch_all=True,
        )
        platforms = run_query(
            conn,
            is_postgres,
            "SELECT DISTINCT platform FROM influencers WHERE platform IS NOT NULL AND platform != ''",
            fetch_all=True,
        )
        categories = run_query(
            conn,
            is_postgres,
            "SELECT DISTINCT category_main FROM influencers WHERE category_main IS NOT NULL AND category_main != ''",
            fetch_all=True,
        )
        sub_categories = run_query(
            conn,
            is_postgres,
            "SELECT DISTINCT category_sub FROM influencers WHERE category_sub IS NOT NULL AND category_sub != ''",
            fetch_all=True,
        )
        conn.close()
        return render_template(
            "index.html",
            influencers=influencers,
            q=filters["q"],
            platform=filters["platform"],
            category_main=filters["category_main"],
            category_sub=filters["category_sub"],
            platforms=[p["platform"] for p in platforms],
            categories=[c["category_main"] for c in categories],
            sub_categories=[c["category_sub"] for c in sub_categories],
            all_columns=filters["all_columns"],
            selected_columns=filters["selected_columns"],
            column_labels=COLUMN_LABELS,
        )

    @app.route("/search")
    @login_required
    def search():
        filters = build_filters(request)
        conn, is_postgres = get_db()
        influencers = run_query(
            conn,
            is_postgres,
            f"""
            SELECT * FROM influencers
            {filters["where_clause"]}
            ORDER BY updated_at DESC
            """,
            filters["params"],
            fetch_all=True,
        )
        platforms = run_query(
            conn,
            is_postgres,
            "SELECT DISTINCT platform FROM influencers WHERE platform IS NOT NULL AND platform != ''",
            fetch_all=True,
        )
        categories = run_query(
            conn,
            is_postgres,
            "SELECT DISTINCT category_main FROM influencers WHERE category_main IS NOT NULL AND category_main != ''",
            fetch_all=True,
        )
        sub_categories = run_query(
            conn,
            is_postgres,
            "SELECT DISTINCT category_sub FROM influencers WHERE category_sub IS NOT NULL AND category_sub != ''",
            fetch_all=True,
        )
        conn.close()
        return render_template(
            "search.html",
            influencers=influencers,
            q=filters["q"],
            platform=filters["platform"],
            category_main=filters["category_main"],
            category_sub=filters["category_sub"],
            platforms=[p["platform"] for p in platforms],
            categories=[c["category_main"] for c in categories],
            sub_categories=[c["category_sub"] for c in sub_categories],
            all_columns=filters["all_columns"],
            selected_columns=filters["selected_columns"],
            column_labels=COLUMN_LABELS,
        )

    @app.route("/discover", methods=["GET"])
    @login_required
    def discover():
        query = request.args.get("q", "").strip()
        platform = request.args.get("platform", "").strip()
        limit = request.args.get("limit", "").strip()
        try:
            limit_value = int(limit) if limit else DEFAULT_DISCOVER_LIMIT
        except ValueError:
            limit_value = DEFAULT_DISCOVER_LIMIT
        limit_value = max(1, min(limit_value, 25))

        results = []
        errors = []

        if query:
            if platform in {"", "YouTube"}:
                yt = search_youtube_channels(query, limit_value)
                if yt["error"]:
                    errors.append(yt["error"])
                results.extend(yt["items"])

            if platform in {"", "Instagram"}:
                ig = search_instagram_profiles(query, limit_value)
                if ig["error"]:
                    errors.append(ig["error"])
                results.extend(ig["items"])

        return render_template(
            "discover.html",
            query=query,
            platform=platform,
            limit=limit_value,
            results=results,
            errors=errors,
        )

    @app.route("/new", methods=["GET", "POST"])
    @login_required
    def new():
        if request.method == "POST":
            form = extract_form(request)
            save_influencer(form)
            flash("새 인플루언서를 추가했습니다.")
            return redirect(url_for("index"))
        return render_template("edit.html", influencer=None)

    @app.route("/edit/<int:influencer_id>", methods=["GET", "POST"])
    @login_required
    def edit(influencer_id):
        conn, is_postgres = get_db()
        influencer = run_query(
            conn,
            is_postgres,
            "SELECT * FROM influencers WHERE id = ?",
            (influencer_id,),
            fetch_one=True,
        )
        conn.close()
        if not influencer:
            abort(404)
        if request.method == "POST":
            form = extract_form(request)
            update_influencer(influencer_id, form)
            flash("수정이 저장되었습니다.")
            return redirect(url_for("index"))
        return render_template("edit.html", influencer=influencer)

    @app.route("/delete/<int:influencer_id>", methods=["POST"])
    @login_required
    def delete(influencer_id):
        conn, is_postgres = get_db()
        run_query(
            conn,
            is_postgres,
            "DELETE FROM influencers WHERE id = ?",
            (influencer_id,),
        )
        conn.commit()
        conn.close()
        flash("삭제되었습니다.")
        return redirect(url_for("index"))

    @app.route("/delete-all", methods=["POST"])
    @login_required
    def delete_all():
        conn, is_postgres = get_db()
        run_query(conn, is_postgres, "DELETE FROM influencers")
        conn.commit()
        conn.close()
        flash("전체 목록이 삭제되었습니다.")
        return redirect(url_for("index"))

    @app.route("/import", methods=["GET", "POST"])
    @login_required
    def import_excel():
        if request.method == "POST":
            file = request.files.get("file")
            if not file or file.filename == "":
                flash("엑셀 파일을 선택해 주세요.")
                return redirect(url_for("import_excel"))
            try:
                df = pd.read_excel(file)
            except Exception:
                flash("엑셀 파일을 읽을 수 없습니다. 파일 형식을 확인해 주세요.")
                return redirect(url_for("import_excel"))

            df = normalize_columns(df)
            if "account_name" not in df.columns:
                flash("필수 컬럼(이름/계정명)이 없습니다.")
                return redirect(url_for("import_excel"))

            df = df.dropna(subset=["account_name"])
            rows = 0
            conn, is_postgres = get_db()
            for _, row in df.iterrows():
                data = {
                    "influencer_id": clean_text(row.get("influencer_id")),
                    "platform": clean_text(row.get("platform")),
                    "category_main": clean_text(row.get("category_main")),
                    "category_sub": clean_text(row.get("category_sub")),
                    "account_name": clean_text(row.get("account_name")),
                    "profile_url": clean_text(row.get("profile_url")),
                    "instagram_username": clean_text(row.get("instagram_username")),
                    "contact_email": clean_text(row.get("contact_email")),
                    "agency": clean_text(row.get("agency")),
                    "followers_raw": clean_text(row.get("followers_raw")),
                    "followers_num": coerce_int(row.get("followers_num")),
                    "follower_range": clean_text(row.get("follower_range")),
                    "video_usage": clean_text(row.get("video_usage")),
                    "target_2030_score": coerce_int(row.get("target_2030_score")),
                    "price_bdc": clean_text(row.get("price_bdc")),
                    "price_ppl": clean_text(row.get("price_ppl")),
                    "price_short": clean_text(row.get("price_short")),
                    "price_ig": clean_text(row.get("price_ig")),
                    "thumbnail_url": clean_text(row.get("thumbnail_url")),
                    "dm_message": clean_text(row.get("dm_message")),
                    "notes": clean_text(row.get("notes")),
                }
                if not data["instagram_username"] and data["profile_url"]:
                    data["instagram_username"] = extract_instagram_username(
                        data["profile_url"]
                    )
                if not data["thumbnail_url"] and data["profile_url"]:
                    data["thumbnail_url"] = fetch_thumbnail_url(data["profile_url"])
                if not data["account_name"]:
                    continue
                run_query(
                    conn,
                    is_postgres,
                    """
                    INSERT INTO influencers (
                        influencer_id, platform, category_main, category_sub,
                        account_name, profile_url, instagram_username, contact_email, agency,
                        followers_raw, followers_num, follower_range, video_usage,
                        target_2030_score, price_bdc, price_ppl, price_short, price_ig,
                        thumbnail_url, dm_message, notes, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        data["influencer_id"],
                        data["platform"],
                        data["category_main"],
                        data["category_sub"],
                        data["account_name"],
                        data["profile_url"],
                        data["instagram_username"],
                        data["contact_email"],
                        data["agency"],
                        data["followers_raw"],
                        data["followers_num"],
                        data["follower_range"],
                        data["video_usage"],
                        data["target_2030_score"],
                        data["price_bdc"],
                        data["price_ppl"],
                        data["price_short"],
                        data["price_ig"],
                        data["thumbnail_url"],
                        data["dm_message"],
                        data["notes"],
                        datetime.utcnow().isoformat(),
                        datetime.utcnow().isoformat(),
                    ),
                )
                rows += 1
            conn.commit()
            conn.close()
            flash(f"{rows}건을 가져왔습니다.")
            return redirect(url_for("index"))
        return render_template("import.html")

    @app.route("/dm/apply", methods=["POST"])
    @login_required
    def apply_dm_template():
        dm_template = request.form.get("dm_template", "").strip()
        if not dm_template:
            flash("DM 문구를 입력해 주세요.")
            return redirect(url_for("index"))

        filters = build_filters_from_values(
            request.form.get("q", ""),
            request.form.get("platform", ""),
            request.form.get("category_main", ""),
            request.form.get("category_sub", ""),
            "",
        )
        if not filters["where_clause"] and request.form.get("confirm_all") != "yes":
            flash("전체 적용은 체크박스를 선택해 주세요.")
            return redirect(url_for("index"))

        conn, is_postgres = get_db()
        count_row = run_query(
            conn,
            is_postgres,
            f"SELECT COUNT(*) as cnt FROM influencers {filters['where_clause']}",
            filters["params"],
            fetch_one=True,
        )
        count = count_row["cnt"] if isinstance(count_row, dict) else count_row[0]
        run_query(
            conn,
            is_postgres,
            f"UPDATE influencers SET dm_message = ? {filters['where_clause']}",
            [dm_template] + filters["params"],
        )
        conn.commit()
        conn.close()
        flash(f"{count}건에 DM 문구를 적용했습니다.")
        return redirect(url_for("index"))

    @app.route("/export")
    @login_required
    def export_excel():
        filters = build_filters(request)
        selected_columns = filters["selected_columns"]
        if not selected_columns:
            flash("내보낼 컬럼을 선택해 주세요.")
            return redirect(url_for("index"))

        columns_sql = ", ".join(selected_columns)
        conn, is_postgres = get_db()
        rows = run_query(
            conn,
            is_postgres,
            f"""
            SELECT {columns_sql} FROM influencers
            {filters["where_clause"]}
            ORDER BY updated_at DESC
            """,
            filters["params"],
            fetch_all=True,
        )
        conn.close()

        data = []
        for row in rows:
            item = {}
            for col in selected_columns:
                item[COLUMN_LABELS.get(col, col)] = row[col]
            data.append(item)

        df = pd.DataFrame(data)
        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        filename = f"influencers_{datetime.utcnow().date().isoformat()}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def extract_form(req):
        return {
            "influencer_id": req.form.get("influencer_id", "").strip(),
            "platform": req.form.get("platform", "").strip(),
            "category_main": req.form.get("category_main", "").strip(),
            "category_sub": req.form.get("category_sub", "").strip(),
            "account_name": req.form.get("account_name", "").strip(),
            "profile_url": req.form.get("profile_url", "").strip(),
            "instagram_username": req.form.get("instagram_username", "").strip(),
            "contact_email": req.form.get("contact_email", "").strip(),
            "agency": req.form.get("agency", "").strip(),
            "followers_raw": req.form.get("followers_raw", "").strip(),
            "followers_num": coerce_int(req.form.get("followers_num")),
            "follower_range": req.form.get("follower_range", "").strip(),
            "video_usage": req.form.get("video_usage", "").strip(),
            "target_2030_score": coerce_int(req.form.get("target_2030_score")),
            "price_bdc": req.form.get("price_bdc", "").strip(),
            "price_ppl": req.form.get("price_ppl", "").strip(),
            "price_short": req.form.get("price_short", "").strip(),
            "price_ig": req.form.get("price_ig", "").strip(),
            "thumbnail_url": req.form.get("thumbnail_url", "").strip(),
            "dm_message": req.form.get("dm_message", "").strip(),
            "notes": req.form.get("notes", "").strip(),
        }

    def save_influencer(data):
        now = datetime.utcnow().isoformat()
        if not data.get("instagram_username") and data.get("profile_url"):
            data["instagram_username"] = extract_instagram_username(
                data["profile_url"]
            )
        if not data.get("thumbnail_url") and data.get("profile_url"):
            data["thumbnail_url"] = fetch_thumbnail_url(data["profile_url"])
        conn, is_postgres = get_db()
        run_query(
            conn,
            is_postgres,
            """
            INSERT INTO influencers (
                influencer_id, platform, category_main, category_sub,
                account_name, profile_url, instagram_username, contact_email, agency,
                followers_raw, followers_num, follower_range, video_usage,
                target_2030_score, price_bdc, price_ppl, price_short, price_ig,
                thumbnail_url, dm_message, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["influencer_id"],
                data["platform"],
                data["category_main"],
                data["category_sub"],
                data["account_name"],
                data["profile_url"],
                data["instagram_username"],
                data["contact_email"],
                data["agency"],
                data["followers_raw"],
                data["followers_num"],
                data["follower_range"],
                data["video_usage"],
                data["target_2030_score"],
                data["price_bdc"],
                data["price_ppl"],
                data["price_short"],
                data["price_ig"],
                data["thumbnail_url"],
                data["dm_message"],
                data["notes"],
                now,
                now,
            ),
        )
        conn.commit()
        conn.close()

    def update_influencer(influencer_id, data):
        now = datetime.utcnow().isoformat()
        if not data.get("instagram_username") and data.get("profile_url"):
            data["instagram_username"] = extract_instagram_username(
                data["profile_url"]
            )
        if not data.get("thumbnail_url") and data.get("profile_url"):
            data["thumbnail_url"] = fetch_thumbnail_url(data["profile_url"])
        conn, is_postgres = get_db()
        run_query(
            conn,
            is_postgres,
            """
            UPDATE influencers SET
                influencer_id = ?,
                platform = ?,
                category_main = ?,
                category_sub = ?,
                account_name = ?,
                profile_url = ?,
                instagram_username = ?,
                contact_email = ?,
                agency = ?,
                followers_raw = ?,
                followers_num = ?,
                follower_range = ?,
                video_usage = ?,
                target_2030_score = ?,
                price_bdc = ?,
                price_ppl = ?,
                price_short = ?,
                price_ig = ?,
                thumbnail_url = ?,
                dm_message = ?,
                notes = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                data["influencer_id"],
                data["platform"],
                data["category_main"],
                data["category_sub"],
                data["account_name"],
                data["profile_url"],
                data["instagram_username"],
                data["contact_email"],
                data["agency"],
                data["followers_raw"],
                data["followers_num"],
                data["follower_range"],
                data["video_usage"],
                data["target_2030_score"],
                data["price_bdc"],
                data["price_ppl"],
                data["price_short"],
                data["price_ig"],
                data["thumbnail_url"],
                data["dm_message"],
                data["notes"],
                now,
                influencer_id,
            ),
        )
        conn.commit()
        conn.close()

    return app


app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
