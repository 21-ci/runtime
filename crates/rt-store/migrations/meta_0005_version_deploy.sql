-- What a deploy says about a version (R4). `meta` is client-claimed: `{"git":{sha,ref,repo,author,message},"label"}`,
-- 4 KiB at most; never read for a decision (`migrations` is the validated list).
ALTER TABLE versions ADD COLUMN meta TEXT CHECK (meta IS NULL OR json_valid(meta));
-- `activate: "after_build"`: who asked to make the version live once its build is ready. Cleared when taken.
ALTER TABLE versions ADD COLUMN activate_after_build TEXT;
