ROOT=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
sqlite3 -header -box "$ROOT/logs/sprt.db" '
SELECT candidate_net, elo
FROM sprt
WHERE reference_net = "nn-0ee0657fb25e.nnue"
  AND candidate_net LIKE "enyo-%"
ORDER BY elo DESC
LIMIT 10;'
