#compute absolute path from a relative path
function absPath() {
  local relPath=$1
  if [[ ! "$relPath" == /* ]]; then
    #relative path
    absolutePath=$(
      cd "$relPath"
      pwd
    )
  fi
  echo "$absolutePath"
}
