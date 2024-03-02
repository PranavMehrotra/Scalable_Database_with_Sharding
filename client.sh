while true; do
  # Take input from user, for add or remove servers
  echo -n "Enter home/rep/add/rm/exit: "
  read action

  # Check if action is add or rm (case insensitive)
  if [ "${action,,}" == "add" ]; then
      echo -n "Enter number of servers to add: "
      read n
      echo -n "Enter hostnames of servers to add (seperated by spaces): "
      read -a hostnames
      # Check if hostnames are valid
      for hostname in "${hostnames[@]}"
      do
          if [[ ! $hostname =~ ^[a-zA-Z0-9_@-]+$ ]]; then
              echo "Invalid hostname"
          fi
      done
      # Check if number of servers is valid
      if [ $n -lt 0 ]; then
          echo "Invalid number of servers"
      fi
      # hostname_string="[\"$(IFS='","'; echo "${hostnames[*]}")\"]"
      hostname_string=""
      for element in "${hostnames[@]}"; do
          hostname_string+="\"$element\","
      done
      hostname_string="${hostname_string%,}"  # Remove trailing comma
      # hostname_string+="]"
      # echo $hostname_string
      # echo $n
      # Send request to server with hostnames and number of servers
      curl -X POST \
      -H "Content-type: application/json" \
      -d "{\"n\":$n, \"hostnames\": [$hostname_string]}" \
      "http://0.0.0.0:5000/add"

      echo ""

  elif [ "${action,,}" == "rm" ]; then
      echo -n "Enter number of servers to remove: "
      read n
      echo -n "Enter hostnames of servers to remove (seperated by spaces): "
      read -a hostnames
      # Check if hostnames are valid
      for hostname in "${hostnames[@]}"
      do
          if [[ ! $hostname =~ ^[a-zA-Z0-9_@-]+$ ]]; then
              echo "Invalid hostname"
          fi
      done
      # Check if number of servers is valid
      if [ $n -lt 0 ]; then
          echo "Invalid number of servers"
      fi

      hostname_string=""
      for element in "${hostnames[@]}"; do
          hostname_string+="\"$element\","
      done
      hostname_string="${hostname_string%,}"  # Remove trailing comma
      # hostname_string+="]"
      # echo $hostname_string
      # Send request to server
      curl -X DELETE \
      -H "Content-type: application/json" \
      -d "{\"n\":$n, \"hostnames\": [$hostname_string]}" \
      "http://0.0.0.0:5000/rm"

      echo ""
  elif [ "${action,,}" == "home" ]; then
      curl -X GET "http://0.0.0.0:5000/home"
      echo ""
  elif [ "${action,,}" == "rep" ]; then
      curl -X GET "http://0.0.0.0:5000/rep"
      echo ""
  elif [ "${action,,}" == "exit" ]; then
      break
  else
      echo "Invalid action"
  fi
done
exit 0