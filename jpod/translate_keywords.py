import pandas as pd
from googletrans import Translator
translator = Translator()

### Technology-Field Keywords from Bloom et al. (2021) --------------------------------------
df = pd.read_csv("data/raw_data/bloom_fields.csv")
df = pd.merge(df, pd.read_csv("data/raw_data/key_words_tech.csv"), on = "bloom_code")
df["keyword_en"] = [w.replace(u'\xa0', u' ') for w in df["keyword_en"]]
for dest in ["de", "fr", "it", "en"]:
    if dest != "en":
        df["keyword_" + dest] = [translator.translate(text = w, dest = dest, src = "en").text for w in df["keyword_en"]]
        df["keyword_" + dest] = df["keyword_" + dest].str.lower()
        print("Translated and lowercased keywords to %s" %dest)
    else:
        df["keyword_" + dest] = df["keyword_" + dest].str.lower()
        print("Lowercased keywords in %s" %dest)
df.loc[(df.bloom_field == "Oled display"), ["keyword_en", "keyword_de", "keyword_fr", "keyword_it"]] = " oled"
df.loc[(df.bloom_field == "Cloud computing") & (df.keyword_it == "pas"), "keyword_it"] = "paas"
df.loc[(df.bloom_field == "Cloud computing") & (df.keyword_it == "saa"), "keyword_it"] = "saas"
df.loc[(df.bloom_field == "Online streaming") & (df.keyword_fr == "direct"), "keyword_fr"] = "transmission en direct"
df.to_csv("data/bloom_tech.csv", index=False)

### AI-Keywords from Acemoglu et al. (2022) --------------------------------------
with open("data/raw_data/acemoglu_ai_keywords.txt") as f:
    KEYWORDS = f.read().split(", ")
    KEYWORDS = [w.lower().strip() for w in KEYWORDS]
df = pd.DataFrame({"keyword_en": KEYWORDS})
for dest in ["de", "fr", "it"]:
    df["keyword_" + dest] = [translator.translate(text = w, dest = dest, src = "en").text.lower() for w in df["keyword_en"]]
    print("Translated and lowercased keywords to %s" %dest)
df.to_csv("data/acemoglu_ai_keywords.csv", index=False)