import psycopg2
import requests
import time

DB_CONFIG = {
    "dbname": "overtune_small",
    "user": "pablo",
    "host": "/var/run/postgresql",
    "port": 5432,
}

SAMPLE_SIZE = 20000  # full dataset

def check_itunes(isrc):
    try:
        r = requests.get(f"https://itunes.apple.com/lookup?isrc={isrc}&entity=song", timeout=10)
        if r.status_code == 200 and r.json().get("resultCount", 0) > 0:
            return True
    except:
        pass
    return False

def check_musicbrainz(isrc):
    try:
        r = requests.get(
            f"https://musicbrainz.org/ws/2/isrc/{isrc}?fmt=json",
            headers={"User-Agent": "OvertuneTester/1.0 (pablo@example.com)"},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            return len(data.get("recordings", [])) > 0
    except:
        pass
    return False

def check_spotify_open(isrc):
    # Spotify requires auth — just flag as N/A
    return None

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print(f"Sampling {SAMPLE_SIZE} ISRCs from overtune_small...")
    cur.execute(f"""
        SELECT ti.isrc
        FROM track_isrc ti
        ORDER BY RANDOM()
        LIMIT {SAMPLE_SIZE};
    """)
    isrcs = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    itunes_hits = 0
    mb_hits = 0
    total = len(isrcs)

    print(f"Testing {total} ISRCs...\n")

    for i, isrc in enumerate(isrcs):
        itunes = check_itunes(isrc)
        if itunes:
            itunes_hits += 1

        mb = check_musicbrainz(isrc)
        if mb:
            mb_hits += 1

        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{total} — iTunes hits: {itunes_hits}, MusicBrainz hits: {mb_hits}")

        time.sleep(0.5)  # respectful rate for testing

    print(f"\n✅ Results for {total} ISRCs:")
    print(f"  iTunes:      {itunes_hits}/{total} ({100*itunes_hits//total}%) found")
    print(f"  MusicBrainz: {mb_hits}/{total} ({100*mb_hits//total}%) found")
    print(f"  Spotify:     requires OAuth — not tested")
    print(f"\nRecommendation: use whichever API has highest hit rate above.")

if __name__ == "__main__":
    main()
