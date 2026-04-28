#!/bin/bash
echo "=== Koast Workers Status ==="
echo ""

for svc in koast-pricing koast-market koast-bookings; do
    timer="${svc}.timer"
    echo "--- ${svc} ---"

    # Timer status
    if systemctl is-active --quiet "$timer" 2>/dev/null; then
        echo "  Timer: ACTIVE"
        next=$(systemctl show "$timer" --property=NextElapseUSecRealtime --value 2>/dev/null)
        echo "  Next run: $next"
    else
        echo "  Timer: INACTIVE"
    fi

    # Last run
    last=$(systemctl show "${svc}.service" --property=ExecMainStartTimestamp --value 2>/dev/null)
    exit_code=$(systemctl show "${svc}.service" --property=ExecMainStatus --value 2>/dev/null)
    echo "  Last run: ${last:-never}"
    echo "  Exit code: ${exit_code:-N/A}"

    # Last log line
    last_log=$(journalctl -u "${svc}.service" --no-pager -n 1 --output=cat 2>/dev/null)
    echo "  Last log: ${last_log:-none}"
    echo ""
done

echo "=== Log files ==="
for f in /var/log/koast/*.log; do
    if [ -f "$f" ]; then
        lines=$(wc -l < "$f")
        size=$(du -h "$f" | cut -f1)
        echo "  $f: $lines lines, $size"
    fi
done
