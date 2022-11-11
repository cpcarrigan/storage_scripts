#!/bin/bash

min_ago=120

find work/active -mmin +${min_ago} -exec mv '{}' work/ready \;
