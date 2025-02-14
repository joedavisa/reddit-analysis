#!/bin/bash
for f in ./csv/*reviews.csv; do
	python3 ./reviews_to_date.py "$f"
done
