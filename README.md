# TV Telegram Bot (TizenBrew service module)

Runs on a Samsung Tizen TV inside TizenBrew. Long-polls Telegram and sends remote keys to the TV itself.
No secrets in this repo: the bot token and chat id are written to `/home/owner/share/tvbot-config.json`
on the TV over the LAN (`POST http://<tv-ip>:8095/config`).

Commands: `/tat`, `/tatsau <min>`, `/huy`, `/trangthai`, `/tieng`, `/to`, `/nho`.
