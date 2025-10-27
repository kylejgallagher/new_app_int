import pandas as pd
import re
import unicodedata


file_scout_int = "applications_from_2024"
file = "JAR_2025-10-27.csv"
# df = pd.read_csv(f"{file_scout_int}.csv")
df = pd.read_csv(file)

# Convert to datetime and sort
df["date_created"] = pd.to_datetime(df["date_created"], errors='coerce')
df = df.sort_values(by=["job_application_id", "resume_id", "date_created"])

# ===== Text Normalization =====
def normalize_text(text):
    if pd.isna(text):
        return ""
    text = str(text)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)  # Replace all whitespace/newlines with single space
    return text.strip()

df["normalized_response"] = df["body"].apply(normalize_text)

# ===== Patterns =====
salutation_pattern = (
    r"面接|来社|カジュアル|面談|カジュアル面談|interview|speak with you|speaking with you|meeting you|casual talk"
    r"invitation|Zoomのインビテーション|online chat|meeting|Zoomリンク|お時間になりましたら|"
    r"Zoom ミーティング|パスコード:|先ほどTeams|https://teams.microsoft.com|下記日時|当日|日時"
)

keyword_pattern = (
    r"面接日|面談日時|当日|開始|confirmed|date:|time:|meeting id|ミーティングid|ミーティング|招待メール|Teams meeting"
    r"meeting link|お会いできる|ビデオ通話のリンク|招待メール|Googleカレンダー|面接中|面接用url|"
    r"設定いたしました|Google invitation|confirmed|お時間になりましたら|Google Meet|楽しみに|"
    r"パスコード:|send you an invitation|https://meet.google.com|ご参加頂けますと|Teamsのリンク|お越し|リンク|場所：|会場|"
    r"Python\s*\d*"
)

exclude_pattern = (
    r"candidate|applicant|sir|madam|sender|候補者|滅失|日程調整|登録会日程|候補日|ご都合のよい|"
    r"ご都合の良い|ご都合が良い|自動送信|〇|ご都合良い|提出締切り|ご都合いかがでしょうか|"
    r"https://outlook.office365.com/owa/calendar|平日|ご都合がいい日|登録日程|面談希望日|HireRight/J.Screen -|"
    r"https://jac|面接可能な日程|面接にお越し頂く際は|日程候補|日時候補|誠に残念ながら|、残念ながら|"
    r"ご希望にそえない|https://itsumen.net/user|可能な日時をご指定|ご都合|any of the following"
)

# ===== Diagnostics =====
df["has_salutation"] = df["normalized_response"].str.contains(salutation_pattern, regex=True, na=False)
df["has_keyword"] = df["normalized_response"].str.contains(keyword_pattern, regex=True, na=False)
df["has_exclude"] = df["normalized_response"].str.contains(exclude_pattern, regex=True, na=False)

print("\n--- Summary Diagnostics ---")
print(f"Total messages: {len(df)}")
print(f"Salutation matches: {df['has_salutation'].sum()}")
print(f"Keyword matches: {df['has_keyword'].sum()}")
print(f"Excluded matches: {df['has_exclude'].sum()}")

# ===== Filter messages directly =====
filtered = df[
    df["has_salutation"] &
    df["has_keyword"] &
    ~df["has_exclude"]
].copy()

# ===== Add flags and type =====
filtered["Direct"] = (filtered.get("employer_type", 0) == 1.0).astype(int)
filtered["type"] = "Application"

# ===== Final columns =====
final = filtered[["job_application_id", "date_created", "employer_id", "division_id", "job_id","resume_id","job_seeker_id", "body", "Direct", "type"]]

# ===== Output =====
print("\n=== Final matched rows ===")
print(final)

print("\n=== Summary ===")
print(f"Total normalized messages: {len(df)}")
print(f"Salutation matches: {df['has_salutation'].sum()}")
print(f"Keyword matches: {df['has_keyword'].sum()}")
print(f"Excluded matches: {df['has_exclude'].sum()}")
print(f"Final matches: {len(final)}")

print(f"Total unique application/resume pairs: {len(final)}")
final.to_csv(f"New_Application_Filter_{file}.csv", index=False)
