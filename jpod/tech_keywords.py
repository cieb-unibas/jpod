import pandas as pd
from googletrans import Translator

# get keywords and techfields
DAT_DIR = "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
df = pd.read_csv(DAT_DIR + "bloom_fields.csv")
df = pd.merge(df, pd.read_csv(DAT_DIR + "key_words_tech.csv"), on = "bloom_code")
df["keyword_en"] = [w.replace(u'\xa0', u' ') for w in df["keyword_en"]]

# translate and lowercase keywords to German, French, and Italian
translator = Translator()
for dest in ["de", "fr", "it", "en"]:
    if dest != "en":
        df["keyword_" + dest] = [translator.translate(text = w, dest = dest, src = "en").text for w in df["keyword_en"]]
        df["keyword_" + dest] = df["keyword_" + dest].str.lower()
        print("Translated and lowercased keywords to %s" %dest)
    else:
        df["keyword_" + dest] = df["keyword_" + dest].str.lower()
        print("Lowercased keywords in %s" %dest)

# save data
df.to_csv(DAT_DIR + "bloom_tech.csv")