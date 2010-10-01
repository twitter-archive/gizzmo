module Gizzard
  module Digest
    def self.fnv1a_64(data)
      prime = 1099511628211
      rv = 0xcbf29ce484222325
      data.each_byte do |byte|
        rv = ((rv ^ (byte & 0xff)) * prime) & 0xffffffffffffffff
      end
      # trim to 60 bits for gizzard.
      rv & 0x0fffffffffffffff
    end
  end
end
