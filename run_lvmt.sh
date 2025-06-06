#!/bin/bash
sudo cgclassify -g memory:/lvmt $$
exec "$@"