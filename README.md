# 인플루언서 리스트 웹페이지

엑셀 기반 인플루언서 리스트를 팀이 함께 보고 수정할 수 있는 간단한 웹앱입니다.

## 기능
- 목록 보기/검색/필터
- 추가/수정/삭제
- 엑셀 업로드로 일괄 가져오기
- 엑셀 내보내기
- 컬럼 선택(숨김/표시)
- 프로필URL 기반 썸네일 자동 수집(가능한 경우)
- 로그인 필요
- 외부 추천 서칭 (YouTube/Instagram)

## 로컬 실행
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python app.py
```
브라우저에서 `http://127.0.0.1:5000` 접속

## 로그인
- 아이디: `spler`
- 비밀번호: `spler123`

## 엑셀 가져오기
`엑셀 가져오기` 메뉴에서 `.xlsx` 파일을 업로드합니다.  
컬럼명은 아래 기준으로 자동 매핑됩니다.

- 인플루언서ID, 플랫폼, 카테고리(대), 카테고리(소)
- 이름/계정명, 프로필URL, 컨택이메일, 에이전시/소속
- 팔로워/구독자(원본), 팔로워/구독자(숫자), 팔로워 구간
- 영상 활용도(高/中/低), 2030 타깃 적합도(1~5)
- 단가_BDC, 단가_PPL, 단가_Short/Shorts, 단가_IG, 비고

## 공유 URL로 배포
간단히 공유하려면 클라우드 서비스(Render/Railway/Fly 등)에 배포할 수 있습니다.

### Render 배포 (가장 간단)
1) Render 가입 후 `New +` → `Web Service`
2) GitHub에 이 폴더를 올린 뒤 레포 선택
3) `render.yaml` 자동 인식 확인
4) 배포 완료 후 URL 공유

### Railway/Fly/Heroku 계열
`Procfile`이 포함되어 있어 기본 설정으로 배포가 가능합니다.

SQLite는 소규모 협업에 적합하며, 동시성이 많아지면 별도 DB로 전환을 권장합니다.

## 외부 추천 서칭 (API 키)
- YouTube: 환경변수 `YOUTUBE_API_KEY`
- Instagram: 환경변수 `SERPAPI_KEY` (SerpAPI 사용)

## 데이터 영구 저장 (PostgreSQL)
Render PostgreSQL을 사용하려면 환경변수 `DATABASE_URL`을 설정하세요.
설정되면 자동으로 PostgreSQL을 사용하고, 미설정 시 SQLite를 사용합니다.
