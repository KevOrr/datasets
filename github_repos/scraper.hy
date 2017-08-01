(import [github_repos.config :as g])

(defn format-repo-getter [user-logins &key
                          contributions-cursor
                          issues-cursor
                          pullrequests-cursor
                          [expand-contributions g.scraper-expand-user-contributions]
                          [expand-issues g.scraper-expand-user-issues]
                          [expand-pullrequests g.scraper-expand-user-pullrequests]]

  (setv user-fields [])

  (when expand-contributions
    (user-fields.append
     (gqn "contributedRepositories"
          [["first" 100]
           ["after" contributions-cursor]]
          (gqn "edges"
               (gqn "nodes")))))

  (when expand-issues
    (user-fields.append
     (gqn "issues"
          [["first" 100]
           ["after" issues-cursor]]
          (gqn "edges"
               (gqn "nodes")))))

  (when expand-pullrequests
    (user-fields.append
     (gqn "pullrequests"
          [["first" 100]
           ["after" pullrequests-cursor]]
          (gqn "edges"
               (gqn "nodes")))))

  (gqn "query"
       (list-comp
        (apply gqn "user" [["login" user_login]]
               user-fields))))
