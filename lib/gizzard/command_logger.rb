module Gizzard
  class CommandLogger
    attr_reader :file, :pos

    def initialize filename
      begin
        @pos = 0
        @file = File.new(filename, "a")
        @file.sync = true
      rescue e
        STDERR.puts "Error opening logfile. #{e}"
      end
    end

    #Write a marshal of an object or of its serialize method if it has one
    def write obj, extra=[]
      if obj.respond_to? :serialize
        dump = Marshal.dump({:operation => [obj.class, obj.serialize], :extras => extra})
      else
        dump = Marshal.dump({:operation => obj, :extras => extra})
      end
      @file.print("#{@pos}\t#{dump}\n\n\n")
      @pos += 1
    end

    def last_command
      $/="\n\n\n"
      command = []
      File.readlines(file.path).reverse_each do |logline|
        lpos, op = logline.split("\t", 2)

        op = Marshal.load(op)
        op[:operation] = op[:operation][0].deserialize(*op[:operation][1]) if op[:operation].to_a[0].kind_of?(Class) && op[:operation][0].respond_to?(:deserialize)

        command << op
        break if lpos == "0"
      end
      $/="\n"
      command
    end
  end
end