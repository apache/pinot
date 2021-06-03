# Compute absolute dir path from a relative path. Requires that the path exist
function absPath() {
  local relPath=$1
  if [[ ! "$relPath" == /* ]]; then
    relPath=$(
      cd "$relPath"
      pwd
    )
  fi
  echo "$relPath"
}
