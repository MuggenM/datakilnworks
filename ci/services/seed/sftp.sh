#!/bin/bash
# Run by atmoz/sftp's entrypoint (/etc/sftp.d) before sshd starts: a fresh key pair for user `user`. The private key goes
# to out/sftp/id (ci/run_test.sh mounts out/ read-only at /services), the public one into the user's authorized_keys.
set -e
rm -rf /out/sftp && mkdir -p /out/sftp
ssh-keygen -q -t ed25519 -N "" -C dkwci -f /out/sftp/id
chmod 644 /out/sftp/id                          # read by the test container's user; a test-only key
mkdir -p /home/user/.ssh
cat /out/sftp/id.pub >> /home/user/.ssh/authorized_keys
chown -R user /home/user/.ssh; chmod 700 /home/user/.ssh; chmod 600 /home/user/.ssh/authorized_keys
