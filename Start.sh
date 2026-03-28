#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate
python app.py &
APP_PID=$!
trap "kill $APP_PID 2>/dev/null; exit" INT TERM
sleep 2
xdg-open http://127.0.0.1:5000
wait $APP_PID
