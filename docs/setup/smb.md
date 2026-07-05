# SMB Setup

The Mac owns the shared job folder:

```text
/Users/ytt/MissAVJobs
```

Windows maps that share to:

```text
M:\
```

The same job file must be visible at both paths:

```text
Mac:     /Users/ytt/MissAVJobs/ktb-096/audio.wav
Windows: M:\ktb-096\audio.wav
```

Keep SMB private to the home network. Do not expose SMB through Cloudflare Tunnel or the public internet.
