require 'hpricot'
require 'pp'
require 'nokogiri'
require 'builder'
require 'fileutils'

# deleting files
FileUtils.rm_rf(Dir.glob('ivy_templates/*'))

parent_pom_xml = File.open("pom.xml")
parsed_parent_pom_xml = Hpricot::XML(parent_pom_xml)

parent_deps = {}

p_v = (parsed_parent_pom_xml/:version).at(0).innerHTML

old_version = p_v

new_version = p_v.to_f + 0.001

value = %x( mvn versions:set -DnewVersion=#{new_version} -DgenerateBackupPoms=false )

p_v = new_version.to_s

(parsed_parent_pom_xml/:dependency).each do |status|
  ver = status.at("version").innerHTML
  if ver.eql?("${project.version}")
    ver = p_v
  end
  parent_deps[status.at("groupId").innerHTML + ":"+ status.at("artifactId").innerHTML] = ver
end

# now lets go project_by_project
final_deps_tree = []

['pinot-api/pom.xml',
 'pinot-broker/pom.xml',
 'pinot-common/pom.xml',
 'pinot-controller/pom.xml',
 'pinot-core/pom.xml',
 'pinot-hadoop/pom.xml',
 'pinot-server/pom.xml',
 'pinot-tools/pom.xml',
 'pinot-transport/pom.xml',
 'pinot-util/pom.xml'
].each do |project|
  project_pom_file = File.open(project)
  parsed_project_pom_file = Hpricot::XML(project_pom_file)

  project_deps = []
  (parsed_project_pom_file/:dependency).each do |status|
    entry = {
      :group_id => status.at("groupId").innerHTML,
      :artifact_id => status.at("artifactId").innerHTML,
      :version => parent_deps[status.at("groupId").innerHTML + ":"+ status.at("artifactId").innerHTML].to_s
    }

    if (status/:exclusion).size > 0
      exclusions = []
      (status/:exclusion).each do |exclusion|
        exclusions.push({
          :group_id => exclusion.at("groupId").innerHTML,
          :artifact_id => exclusion.at("artifactId").innerHTML
        })
      end
      entry[:exclusions] = exclusions
    end

    project_deps.push(entry)
  end
  final_deps_tree.push({
    :project => project.split("/")[0],
    :deps_tree => project_deps
  })
end

base_output_dir = "ivy_templates"

final_deps_tree.each do |project|
  x = Builder::XmlMarkup.new(:indent => 1)
  x.instruct!
  a = 'ivy-module'
  x.tag!('ivy-module', "version" => "2.0", "xmlns:m" => "http://ant.apache.org/ivy/maven") {
    x.info("organisation" => "com.linkedin.pinot","module" => project[:project], "revision" => p_v) {

    }
    x.dependencies {
      project[:deps_tree].each do |dep|
        if (dep[:exclusions])
          x.dependency("org" => dep[:group_id], "name" => dep[:artifact_id], "rev" => dep[:version]) {
            dep[:exclusions].each do |exclude|
              x.exclude("org" => exclude[:group_id], "module" => exclude[:artifact_id], "name" => "*", "type" => "*", "ext" => "*", "conf" => "", "matcher" => "exact")
            end
          }
        else
          x.dependency("org" => dep[:group_id], "name" => dep[:artifact_id], "rev" => dep[:version])
        end
      end
    }  
  }
  
  data = x.target!
  file = File.new("ivy_templates/"+project[:project] + "-#{p_v}" + ".xml", "wb")
  file.write(data)
  file.close
end

%x( git add -u ivy_templates/)

%x( git add ivy_templates/ pom.xml pinot-api/pom.xml pinot-broker/pom.xml pinot-common/pom.xml pinot-controller/pom.xml pinot-core/pom.xml pinot-hadoop/pom.xml pinot-server/pom.xml pinot-tools/pom.xml pinot-transport/pom.xml pinot-util/pom.xml)

%x( git commit -m "updating to #{new_version}" )

%x( git tag -a v#{new_version} -m "Created tag for #{new_version}")

%x(git push origin v#{new_version})

DIFF = `git log v0.002... --pretty=format:"%h - %an, %ar : %s"`

File.open("CHANGELOG", 'rw') { |file| file.write(DIFF) }