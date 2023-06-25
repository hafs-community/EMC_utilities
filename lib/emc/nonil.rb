module EMC
  module NoNil
    def nonil(s)
      # A simple function that converts nil to '' and returns a string
      return '' if s==nil
      tos=s.to_s
      return '' if tos==nil
      return tos
    end
  end
end
