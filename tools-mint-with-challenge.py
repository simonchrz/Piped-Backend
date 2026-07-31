# Praegt ein po_token als Antwort auf eine Aufgabe, die YouTube UNSERER
# angemeldeten Sitzung gestellt hat.
#
# Bisher holte der bgutil-Container die Aufgabe selbst — anonym (im Log:
# "Using challenge from /att/get"). Das Token beantwortete damit die Frage an
# einen fremden Besucher. Hier holen wir die Aufgabe mit unseren Cookies und
# reichen sie samt Sitzungskontext weiter.
import json, re, sys, urllib.request

COOKIES = "/home/simon/piped/youtube-cookies.txt"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
CV = "2.20260122.01.00"

def cookie_header():
    out = []
    for line in open(COOKIES):
        line = line.strip()
        if not line or line.startswith("#"): continue
        p = line.split("\t")
        if len(p) >= 7 and "youtube.com" in p[0]:
            out.append(p[5] + "=" + p[6])
    return "; ".join(out)

def post(url, payload, headers, timeout=30):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers)
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read().decode())

ck = cookie_header()
html = urllib.request.urlopen(urllib.request.Request(
    "https://www.youtube.com", headers={"Cookie": ck, "User-Agent": UA}), timeout=25).read().decode("utf8","ignore")
vd = re.search(r"\"visitorData\":\"([\w%-]+)\"", html).group(1)
ds = re.search(r"\"DATASYNC_ID\":\"([^\"|]+)", html)
ds = ds.group(1) if ds else None
binding = ds or vd

ctx = {"client": {"clientName": "WEB", "clientVersion": CV, "visitorData": vd, "hl": "de", "gl": "DE"}}
att = post("https://www.youtube.com/youtubei/v1/att/get?prettyPrint=false",
           {"engagementType": "ENGAGEMENT_TYPE_UNBOUND", "context": ctx},
           {"Content-Type": "application/json", "Cookie": ck, "User-Agent": UA,
            "X-Goog-Visitor-Id": vd, "X-Youtube-Client-Name": "1",
            "X-Youtube-Client-Version": CV, "Origin": "https://www.youtube.com"})
challenge = att.get("bgChallenge") or att.get("challenge")

out = post("http://172.18.0.3:4416/get_pot",
           {"content_binding": binding, "challenge": challenge,
            "innertube_context": ctx, "bypass_cache": True},
           {"Content-Type": "application/json"})
print(json.dumps({"poToken": out.get("poToken"), "binding": binding[:12] + "…",
                  "bindingArt": "datasyncId" if ds else "visitorData",
                  "visitorData": vd, "challengeLaenge": len(json.dumps(challenge)) if challenge else 0}))
