feed = '''
<Feed feed{stream_id}.ffm>
File /tmp/feed{stream_id}.ffm
FileMaxSize 100K
ACL allow 127.0.0.1
</Feed>

<Stream audio{stream_id}.mp3>
Feed feed{stream_id}.ffm
Format mp2
AudioCodec libmp3lame
AudioBitRate 48
AudioChannels 1
AudioSampleRate 16000
AVOptionAudio flags +global_header
NoVideo
</Stream>'''


for i in range(105):
    print feed.format(stream_id=i)
