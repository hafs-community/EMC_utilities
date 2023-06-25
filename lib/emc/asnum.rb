module EMC
  module AsNum
    module_function
    def asnum(s)
      # Convert s to a number, returning 0 if you can't.
      begin
        begin
          return s.to_f()
        rescue
          return Float(s)
        end
      rescue
        return 0
      end
    end
  end
end
