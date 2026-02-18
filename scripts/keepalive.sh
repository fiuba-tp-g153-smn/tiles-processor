#!/bin/sh
set -e

# =================================================================================================
# Keep-Alive Wrapper Script
# =================================================================================================
# This script executes a setup command and then keeps the container running indefinitely.
# It is designed to solve two problems:
# 1. Prevent containers from exiting (which orchestrators like Coolify might view as a failure).
# 2. Handle shutdown signals (Ctrl+C / SIGTERM) gracefully.
# Usage: ./keepalive.sh [command_to_run]
# Example: ./keepalive.sh "/setup.sh"

COMMAND="$1"

# 1. Run the actual setup command
if [ -n "$COMMAND" ]; then
    echo "Running setup command: $COMMAND"
    sh -c "$COMMAND"
else
    echo "No command provided, just keeping alive..."
fi

# 2. Create a flag file to indicate readiness (for Docker healthchecks)
echo "Setup complete. Marking readiness at /tmp/setup_done"
touch /tmp/setup_done

# 3. Define Signal Trapping
# We trap SIGINT (Ctrl+C) and SIGTERM (docker stop).
# When caught, we print a message and exit with status 0 (success).
trap 'echo "Signal received, shutting down..."; exit 0' INT TERM

# 4. Enter Infinite Wait Loop (The "Keep-Alive")
# - `tail -f /dev/null` is a dummy process that waits forever.
# - `&` puts it in the background so our shell can continue to listen for signals.
# - `wait` pauses the script until the background process ends (or a signal is received).
echo "Container is now keeping alive. Waiting for signals..."
tail -f /dev/null &
wait $!
