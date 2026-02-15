The database table is called "pg-drizzle_videos"
An example of the database table is in #pg-drizzle_videos.csv

# Part 1

Write a function to retrieve the location of the oldest 'queued' job. Steps:
Check if there are any rows in the database whose status is 'queued'.
If there is, select the oldest one (you can use the 'created_at' property).
Update that entry so that the status is set to 'processing'.
Return the location of this file (the 'blob' attribute from the database), as well as the id of the entry (we will need this later)

# Part 2

Write a function to take a blob location and fetch the file. Steps:
Connect to the blob storage and download the video file using the 'blob' property from the database entry
Return this file so that it can be used by the rest of the AI processing functions

# Part 2

Write a function to upload a video file to blob storage. Steps:
Connect to the blob storage and upload the file, make sure to give it a unique name to avoid conflicting file names.
Return the location of the file (strip the url, so only return the folder (if any) and filename)

# Part 3

Write a function to update the status in the database. Steps:
Connect to the db.
Find the entry by it's id.
Update the status of the entry to 'done'
Update the value of 'completed_blob' to the location of the new video file.
