Synthetic two-second portrait video (180 × 320 test pattern, H.264, no audio),
made with FFmpeg for offline X player layout and metadata tests. No third-party
content.

```sh
ffmpeg -f lavfi -i 'testsrc2=size=180x320:rate=12:duration=2' \
  -an -c:v libx264 -preset veryslow -crf 32 -pix_fmt yuv420p \
  -movflags +faststart portrait.mp4
```
