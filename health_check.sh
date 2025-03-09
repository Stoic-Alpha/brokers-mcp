#!/bin/bash
failed=0
check_service() {
    if nc -z localhost "$1" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

check_service 8001 || failed=1
check_service 8002 || failed=1
check_service 8003 || failed=1

if [ $failed -eq 1 ]; then
    echo "One or more services are not running"
    exit 1
fi

echo "All services are running"
exit 0