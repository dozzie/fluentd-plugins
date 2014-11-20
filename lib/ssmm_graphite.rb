#!/usr/bin/ruby

def split(time, tag, record)
  if record.has_key?('v') && record.has_key?('event') && record['event'].has_key?('vset')
    yield time, 'metric', {
      'metric' => "#{record['location']['host']}.#{record['event']['name']}",
      'value'  => record['event']['vset']['value']['value'],
    }
  end
end
