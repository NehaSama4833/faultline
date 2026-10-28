import json, sys

def main(path):
    d = json.load(open(path))
    print("config:", d["config"])
    if "matrix" in d:
        pols = list(dict.fromkeys(r["policy"] for r in d["matrix"]))
        print("\nSINGLE FAULT AT 30% (200 requests each): success / attempts per request / p99 s")
        print("%-10s" % "fault" + "".join("%-30s" % p for p in pols))
        for f in dict.fromkeys(r["fault"] for r in d["matrix"]):
            row = "%-10s" % f
            for p in pols:
                r = next(x for x in d["matrix"] if x["fault"] == f and x["policy"] == p)
                row += "%-30s" % ("%.3f / %.2f / %.2f" % (r["success"], r["attempts"], r["p99"]))
            print(row)
    print("\nRETRY STORM (offered 16 req/s, capacity 25/s, outage s5-9): per seed")
    pols = list(dict.fromkeys(r["policy"] for r in d["storm"]))
    for p in pols:
        rs = [r for r in d["storm"] if r["policy"] == p]
        print("%-20s success %s | amplification %s | peak arrivals/s %s | recovery_s %s" % (
            p, [round(r["overall_success"], 3) for r in rs], [round(r["amplification"], 2) for r in rs],
            [r["peak_arrivals_per_s"] for r in rs], [r["recovery_s"] for r in rs]))
    r = next(x for x in d["storm"] if x["policy"] == "immediate_x4")
    print("\nsuccess fraction per arrival second, immediate_x4 seed 0:", [round(x, 2) for x in r["success_frac_per_s"]])

if __name__ == "__main__":
    main(sys.argv[1])
