FROM google/cloud-sdk:latest

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# --- Create or modify a non-root user and grant sudo privileges ---
RUN apt-get update && apt-get install -y sudo \
    && EXISTING_USER=$(getent passwd $USER_UID | cut -d: -f1) \
    # Check if a user with the desired UID already exists
    && if [ -n "$EXISTING_USER" ]; then \
        # User exists, check if it has the right name
        if [ "$EXISTING_USER" != "$USERNAME" ]; then \
            echo "User with UID $USER_UID exists ($EXISTING_USER), renaming to $USERNAME." ; \
            # Rename user and move home directory
            usermod -l $USERNAME -d /home/$USERNAME -m $EXISTING_USER ; \
            # Check if a group exists with the old username and rename it
            if getent group $EXISTING_USER >/dev/null; then groupmod -n $USERNAME $EXISTING_USER; fi ; \
        fi \
    else \
        # User does not exist, so create it
        echo "No user with UID $USER_UID found, creating new user $USERNAME." ; \
        # Ensure group exists
        if ! getent group $USER_GID >/dev/null; then groupadd --gid $USER_GID $USERNAME; fi ; \
        # Create the user
        useradd --uid $USER_UID --gid $USER_GID -m $USERNAME ; \
    fi \
    # Add the user to the sudoers file
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME