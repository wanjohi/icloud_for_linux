# Icloud For Linux

Icloud for linux is a python module that lets you sync your icloud files locally using icloud webservices. It uses [pyicloud](https://github.com/picklepete/pyicloud) with some minor modifications to interact with icloud web services.

### Note
This is an unofficial tool, and not affiliated with Apple. USE AT YOUR OWN RISK!

## Installation
Icloud for linux uses the keyring to store your apple password. The first time you run the program, you will be prompted to enter your icloud password. Future runs will fetch the password from the keyring.

### First Run
Run in terminal
```
./icloud --username=your_email@icloud.com
```

### Installing service
Copy `icloud_for_linux.service` to `~/.config/systemd/user/` and modify `ExecStart` to point to the location you installed the icloud for linux binary.

```
systemctl --user enable icloud_for_linux
systemctl --user start icloud_for_linux
```
