#!/usr/bin/env ruby

require 'yaml'
require 'open3'

CURRENT_DIR = Dir.pwd
RB_CONFIG_FILE = CURRENT_DIR + "/.rbconfig"

@gitli_repo = ""
@rb_url = ""
@rb_users = ""
@rb_groups = ""
@user_name = ""

def fetch_conf
  yml_file = YAML.load_file(RB_CONFIG_FILE)
  @gitli_repo = yml_file.fetch "gitli_repo"
  @rb_url = yml_file.fetch "rb_url"
  @rb_users = yml_file.fetch "rb_users"
  @rb_groups = yml_file.fetch "rb_groups"
end

def init
  if !File.exist?(RB_CONFIG_FILE)
    abort("RB CONFIG DOES NOT EXIST")
  end
  fetch_conf
  if @gitli_repo.length == 0 || @rb_url.length == 0 || (@rb_users.length == 0 || @rb_groups.length == 0)
    abort("missing configs in .rbconfig")
  end
end


def run_cmd(query)
  puts "running query : #{query}"
  resp = ""
  IO.popen(query).each do |line|
    resp = resp +  "#{line}"
  end.close
  resp
end

def compare_difs
  repo1 = "git@gitli.corp.linkedin.com:pinot/mirror-pinot.git"
  query1 = "git ls-remote #{repo1}"

  repo2 = "git@github.com:linkedin/pinot.git"
  query2 = "git ls-remote #{repo2}"

  resp1 = run_cmd query1 

  resp2 = run_cmd query2

  puts "***********************"
  sha1 = resp1.split("HEAD")[0]
  sha2 = resp2.split("HEAD")[0]

  puts sha1
  puts sha2
  if (sha1 != sha2)
    abort("need to sync the two repos first")
  end

  if (sha1 == sha2)
    puts " shas match, lets move on with creating the rb "
  end
end

def compute_diff(sha)

end


repo = "git@github.com:linkedin/pinot.git"
query = "git ls-remote #{repo}"

resp = run_cmd query
remote_sha = resp.split("HEAD")[0]

puts remote_sha
