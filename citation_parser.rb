require 'anystyle'

ARGV.each do|reference|
parsed_version = AnyStyle.parse "#{reference}"

require 'json'
json_version = parsed_version.to_json

puts json_version
end