#compute absolute path from a relative path
function absPath() {
  local relPath=$1
  if [[ ! "$relPath" == /* ]]; then
    #relative path
    relPath=$(
      cd "$relPath"
      pwd
    )
  fi
  echo "$relPath"
}
