#!/bin/bash

HOMEPBS=/home-pbs/$USER
X509_USER_PROXY="/tmp/x509up_u$(id -u)"

# Check if the proxy file exists and is valid
if [ -f "$X509_USER_PROXY" ]; then
    if voms-proxy-info --file "$X509_USER_PROXY" --exists --valid 2:00 ; then
        echo "Proxy is valid."
        echo "Copying proxy to local directory."
        mkdir -p $HOMEPBS/proxy
        cp $X509_USER_PROXY $HOMEPBS/proxy/x509up
    else
        echo "Proxy is invalid or expired."
        exit 1
    fi
else
    echo "Proxy file does not exist."
    exit 1
fi

echo "Ready."
echo "Don't forget to refresh the proxy if needed".
