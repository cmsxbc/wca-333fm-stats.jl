#!/bin/bash

julia --project=. -e 'include("WCAStats.jl")'  -- --profile "$@"
