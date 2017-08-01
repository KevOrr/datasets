(defn graphql-node [name &rest args]
  (if (isinstance (first args) list)
    (tuple name
           (remove (fn [pair]
                     (or (none? pair) (none? (get pair 1))))
                   (first args))
           (rest args))
    (tuple name None args)))
