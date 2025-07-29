    static void updateRole() {
        try {
            if (isShuttingDown) return;
            
            List<String> children = curClient.getChildren().usingWatcher(new RoleWatcher()).forPath(mainArgs[3]);
            Collections.sort(children);
            String primaryChild = children.get(0);
            String myChild = myZnode.substring(mainArgs[3].length() + 1);
            boolean isPrimary = myChild.equals(primaryChild);
            
            handler.setPrimary(isPrimary);
            
            if (isPrimary) {
                log.info("I am the primary");
                if (children.size() > 1) {
                    String backupChild = children.get(1);
                    byte[] backupData = curClient.getData().forPath(mainArgs[3] + "/" + backupChild);
                    String[] backupHostPort = new String(backupData).split(":");
                    handler.setBackupAddress(backupHostPort[0], Integer.parseInt(backupHostPort[1]));
                } else {
                    handler.setBackupAddress(null, -1);
                }
            } else {
                log.info("I am the backup");
                // Get state from primary
                try {
                    byte[] primaryData = curClient.getData().forPath(mainArgs[3] + "/" + primaryChild);
                    String[] primaryHostPort = new String(primaryData).split(":");
                    
                    TSocket sock = new TSocket(primaryHostPort[0], Integer.parseInt(primaryHostPort[1]));
                    sock.setTimeout(10000);
                    TTransport transport = new TFramedTransport(sock);
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
                    KeyValueService.Client primaryClient = new KeyValueService.Client(protocol);
                    
                    Map<String, String> state = primaryClient.getCurrentState();
                    handler.syncState(state);
                    transport.close();
                    log.info("Synced state from primary, size=" + state.size());
                } catch (Exception e) {
                    log.error("Failed to sync state from primary", e);
                }
            }
        } catch (Exception e) {
            if (!isShuttingDown) {
                log.error("Error in updateRole", e);
            }
        }
    }