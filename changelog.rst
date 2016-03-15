Changelog
=========

v1.0.0 (2016-03-15)
-------------------

Changes
~~~~~~~

- Restart pyinotify if no data comes in. [Martin Raspaud]

Fix
~~~

- Bugfix: quality is now double to avoid json complaining. [Martin
  Raspaud]

- Bugfix: missed some new metadata name. [Martin Raspaud]

- Bugfix: clean up when a new scene comes in did not initialize the old
  event. [Martin Raspaud]

- Bugfix: Not assuming mirror section name is hostname. [Martin Raspaud]

- Bugfix: don't crash if mirrored data is empty. [Martin Raspaud]

- Bugfix: step count was wrong. [Martin Raspaud]

- Bugfix: Don't close the current stream when a random files is closed.
  [Martin Raspaud]

- Bugfix: None file pointer on closing doesn't crash anymore. [Martin
  Raspaud]

- Bugfix: satellite name is not a datetime anymore :( [Martin Raspaud]

- Bugfix: mirrorwatcher should forward the next pass. [Martin Raspaud]

- Bugfix: forgotten import. [Martin Raspaud]

- Bugfix: send_and_recv send an unpacked message. [Martin Raspaud]

- Bugfix: wrong timeout for mirror get_data. [Martin Raspaud]

- Bugfix: don't crash when file disappears. [Martin Raspaud]

- Bugfix: filewatcher was restarting from the begining of file. [Martin
  Raspaud]

- Bugfix: Add quality flag in mirrorwatchers. [Martin Raspaud]

Other
~~~~~

- Update changelog. [Martin Raspaud]

- Bump version: 0.2.0 → 1.0.0. [Martin Raspaud]

- Add release and changlog config files. [Martin Raspaud]

- Change version string format. [Martin Raspaud]

  This way bumpspec can change it.

- Make next_pass info a dict. [Martin Raspaud]

- Add end_time to metadata on pytroll message. [Martin Raspaud]

- Try to retrieve line from other sources if failed. [Martin Raspaud]

- Add ping_times to all scanlines. [Martin Raspaud]

- Lower times are better. [Martin Raspaud]

- Bugfix. [Martin Raspaud]

- Sort servers by ping times (after quality) [Martin Raspaud]

- Consider a pass done after 1 minute without any data reception.
  [Martin Raspaud]

- Fix zmq version checking. [Martin Raspaud]

- Make trollcast more robust. [Martin Raspaud]

- Change mail sending to CRITICAL. [Martin Raspaud]

- Don't request from jammed servers. [Martin Raspaud]

- Make Requester a subclass of Simplerequester, and centralize context.
  [Martin Raspaud]

- Reference time computation is now based on perfect scanlines only.
  [Martin Raspaud]

- Log when the requester lock is release. [Martin Raspaud]

- Send an email on server jamming. [Martin Raspaud]

- Change to new metadata standard. [Martin Raspaud]

- Bugfix line skip threshold setting. [Martin Raspaud]

- Better handling of mirror's next pass compared to current server pass.
  [Martin Raspaud]

- Allow repeated open/close events on a file being watched. [Martin
  Raspaud]

- Quiet down the message_broadcaster debug info. [Martin Raspaud]

- Update usages. [Martin Raspaud]

- Update documentation. [Martin Raspaud]

- Allow output_file to contain a trollsift string. [Martin Raspaud]

- Bugfix message sending. [Martin Raspaud]

- Bugfix message sending. [Martin Raspaud]

- Bugfix join. [Martin Raspaud]

- Bugfix publisher. [Martin Raspaud]

- Add publishing feature for completed files on the client. [Martin
  Raspaud]

- Take care of missing scanlines. [Martin Raspaud]

- Take care of noisy timecodes. [Martin Raspaud]

- Tweaking noise conditions. [Martin Raspaud]

- Discriminate lines not starting with the frame sync. [Martin Raspaud]

- Add support for kongsberg schedules. [Martin Raspaud]

- Style: remove unused functions. [Martin Raspaud]

- Measure steps in avhrr image for better quality estimation. [Martin
  Raspaud]

- Style, follow pep8. [Martin Raspaud]

- Try to get a more reliable time for the pass. [Martin Raspaud]

- Bugfixes. [Martin Raspaud]

- Cleanup. [Martin Raspaud]

- Use trollsift to read the satellite name from the file. [Martin
  Raspaud]

- Bugfix timedelta > 0. [Martin Raspaud]

- Add support for outputing an error when expected pass is not started.
  [Martin Raspaud]

- Get next pass on hearbeat. [Martin Raspaud]

- Take in ongoing pass as next pass. [Martin Raspaud]

- Get next pass on file closing. [Martin Raspaud]

- Fix unittests for new api, and add tests for client.py. [Martin
  Raspaud]

- Allow get_all method to be stopped. [Martin Raspaud]

- Error handling and debug message. [Martin Raspaud]

- Server was not updating position in case of unplanned datetime.
  [Martin Raspaud]

- Move the monitoring of the file modification to the beginning of the
  fun. [Martin Raspaud]

- Add debug message to notify on file modification. [Martin Raspaud]

- Compare time against time as opposed to time against string. [Martin
  Raspaud]

- Bugfix, _next_pass not next_pass. [Martin Raspaud]

- Add the list of satellites we take in. [Martin Raspaud]

- Adding the schedule reader to the Event handler. [Martin Raspaud]

- Schedule reading module added. [Martin Raspaud]

- Add support for schedule files. [Martin Raspaud]

  * Reads a schedule file if present to determine the next_pass (heartbeat)
  * Sets the correct satellite name if the next_pass is defined.

- Bugfix. [Martin Raspaud]

- Change the conditions for writing a file to disk to limit orphan
  files. [Martin Raspaud]

- Change the client_timeout to 45 seconds. [Martin Raspaud]

- Send email only once a day. [Martin Raspaud]

- Bugfix. [Martin Raspaud]

- Bugfix. [Martin Raspaud]

- Add a heartbeat timeout in the case of mirroring. [Martin Raspaud]

- Add some logging functionnality. [Martin Raspaud]

- Adding mock for tests. [Martin Raspaud]

- Adding the tests package. [Martin Raspaud]

- Cleaning up. [Martin Raspaud]

- Testing first draft. [Martin Raspaud]

- Add the log file option. [Martin Raspaud]

- Switch from watchdog to pyinotify and make newserver the default.
  [Martin Raspaud]

- Take care of disappearing files. [Martin Raspaud]

- Fix pil import. [Martin Raspaud]

- Be carefull when handling bad quality lines, and work in interative
  way. [Martin Raspaud]

- Posttroll-free subscriber in server. [Martin Raspaud]

- Support for line quality in server. [Martin Raspaud]

- Support for elevation in server. [Martin Raspaud]

- Support quality score in client. [Martin Raspaud]

- Talk about nohup in the documentation. [Martin Raspaud]

- Lower interval for heartbeat timer to 40 seconds. [Martin Raspaud]

- Another client bugfix (urlsplit not imported) [Martin Raspaud]

- Bugfix client. [Martin Raspaud]

- Replace the posttroll subscriber by something simpler. [Martin
  Raspaud]

- Start filewatcher only if the data_dir exists. [Martin Raspaud]

- Better logging. [Martin Raspaud]

- More debug. [Martin Raspaud]

- Make sure the server always sends a reply. [Martin Raspaud]

- Moving the lock around. [Martin Raspaud]

- Do no write out binary data in the log. [Martin Raspaud]

- Adding debug info to the client. [Martin Raspaud]

- Bugfix. [Martin Raspaud]

- Support unknown requests also... [Martin Raspaud]

- Use just one context and use lock for notice messages also. [Martin
  Raspaud]

- Add support for notice messages. [Martin Raspaud]

- Bugfix, cleaning set to every minute. [Martin Raspaud]

- Add caching to mirror serving and show mirror heartbeats. [Martin
  Raspaud]

- Don't hardcode the data path... and see where it goes wrong... [Martin
  Raspaud]

- Add cleaning to the new server. [Martin Raspaud]

- Implemented mirroring. [Martin Raspaud]

- New server implementation. [Martin Raspaud]

- Adding lock to reply sending. [Martin Raspaud]

- Fixing logger name. [Martin Raspaud]

- Add a constant_writer for test purposes. [Martin Raspaud]

- Support missing messages from server in client.py. [Martin Raspaud]

- Bugfix. [Martin Raspaud]

- Send "missing" message if the requested scanline doesn't exist on
  server. [Martin Raspaud]

- More debug info on server. [Martin Raspaud]

- Don't crash when we didn't get any lines. [Martin Raspaud]

- Do not forwath the heartbeat (should we ?) [Martin Raspaud]

- Add swap keyword to the read_file function (hrpt reader) [Martin
  Raspaud]

- Fix the client. [Martin Raspaud]

- Updated documentation. [Martin Raspaud]

v0.2.0 (2013-11-04)
-------------------

- Bump up version number. [Martin Raspaud]

- Do the satellite check after the hrpt_sync check for sanity. [Martin
  Raspaud]

- Don't use the filename to know the year of the hrpt data. [Martin
  Raspaud]

- Now reads satellite id from hrpt frame instead. [Martin Raspaud]

- Bugfix. [safusr.u]

- Finalize heartbeating. [Martin Raspaud]

- Add hearbeat functionnality. [Martin Raspaud]

- Corrected version number to be compatible with semver. [Martin
  Raspaud]

- Update documemtation. [Martin Raspaud]

- Correct the tle flag in configuration documentation. [Martin Raspaud]

- Add a few missing dependencies. [Martin Raspaud]

- Add file_pattern to smhi config file. [Martin Raspaud]

- Update doc. [Martin Raspaud]

- Fix logging. [Martin Raspaud]

- Update the doc. [Martin Raspaud]

- Update the doc. [Martin Raspaud]

- Update the doc. [Martin Raspaud]

- Update the doc. [Martin Raspaud]

- Update doc. [Martin Raspaud]

- Add the API to the doc. [Martin Raspaud]

- Update doc. [Martin Raspaud]

- Update .gitignore. [Martin Raspaud]

- Update documentation. [Martin Raspaud]

- Add a setup.py and version.py. [Martin Raspaud]

- Add client and server scripts. [Martin Raspaud]

- Add documentation. [Martin Raspaud]

- Merge branch 'master' of https://github.com/mraspaud/trollcast.
  [Martin Raspaud]

- Initial commit. [Martin Raspaud]

- Style: trollcast. [Martin Raspaud]

- Trollcast update. [Martin Raspaud]

   * renamed mirrorserver to server
   * added some documentation
   * made client executable.


- Added caching to the trollcast server. [Martin Raspaud]

- Added the possibility to gather all scanlines. [Martin Raspaud]

- Merge branch 'master' of github.com:mraspaud/pytroll. [safusr.u]

- Merge branch 'master' of github.com:mraspaud/pytroll. [Martin Raspaud]

- Moved to json formating for scanline requests. [Martin Raspaud]

- Added coordinates of norrköping. [safusr.u]

- Merge branch 'master' of git://github.com/mraspaud/pytroll. [safusr.u]

- Added elevation computation, and logging. [Martin Raspaud]

- Adjusted settings for smhi sattorrent. [safusr.u]

- Sattorrent: Adding mirroring capabilities to the server. [Martin
  Raspaud]

- Cleaning up sattorrent. [Martin Raspaud]

- Option for saving image to file. [Lars Orum Rasmussen]

- Making Python 2.5 happy. [Lars Orum Rasmussen]

- Adding sattorrent. [Martin Raspaud]


