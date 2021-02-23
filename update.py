"""
press any key to show
Is data today updated
Last update time
"""

import os
import time
import keyboard

is_updated = False
log_interval = 600
start_time = time.time()
last_update_time = "No previous update record."
status = "`is_updated` = {}".format(is_updated)

print("Press any key to check the current status.")
while True:
    time_now = time.strftime("%H:%M")
    if time_now == "17:30" and not is_updated:
        last_update_time = time.strftime('%Y-%m-%d %H:%M:%S')
        print("[{}] Start updating data.".format(last_update_time))

        os.system("cd crawler && python stock_trading_history_update.py")
        print("{} done.".format("stock_trading_history_update.py"))
        os.system("cd crawler && python twse_index_update.py")
        print("{} done.".format("twse_index_update.py"))

        is_updated = True
        status = "`is_updated` = {}".format(is_updated)

    if time_now == "00:00":
        is_updated = False
        status = "`is_updated` = {}".format(is_updated)

    if time.time() - start_time >= log_interval:
        start_time = time.time()
        print("[{}] Await updating data.".format(time.strftime('%Y-%m-%d %H:%M:%S')))

    if keyboard.read_key():
        print("[{}] Show current Status.\n{}\nStatus: {:>35}\nLast update time:{:>32}\n{}\n{}".format(
            time.strftime('%Y-%m-%d %H:%M:%S'),
            "=" * 58,
            status,
            last_update_time,
            "=" * 58,
            "[is_updated = False -> not update yet, wait for 17:30]\n" +
            "[is_updated = True -> updated already, wait for 00:00 to reset as False]\n"
        ))
        time.sleep(0.5)
