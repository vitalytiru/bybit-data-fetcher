{
  pkgs,
  lib,
  self,
  ...
}:
{
  boot.loader.grub.enable = true;

  boot.initrd.availableKernelModules = [
    "virtio_net"
    "virtio_pci"
    "virtio_blk"
    "virtio_scsi"
    "ahci"
    "sd_mod"
  ];

  networking.networkmanager.enable = true;
  networking.useDHCP = false;
  networking.firewall.allowedTCPPorts = [
    22
    8123
    9000
  ];

  services.openssh = {
    enable = true;
    settings = {
      PermitRootLogin = "yes";
      PasswordAuthentication = true;
    };
  };
  environment.systemPackages = with pkgs; [
    vim
    wget
    curl
    htop
    git
    iproute2
    netcat
    bind.dnsutils
    net-tools
    mtr
    traceroute
    tcpdump
    nmap
    ethtool
    networkmanager
    zsh
  ];

  networking.networkmanager.ensureProfiles.profiles = {
    "Wired connection 1" = {
      connection = {
        id = "Wired connection 1";
        type = "ethernet";
        interface-name = "ens3";
      };
      ipv4 = {
        method = "manual";
        address1 = "
146.103.115.76/24,146.103.115.1";
        dns = "8.8.8.8;1.1.1.1;";
      };
    };
  };
  systemd.services.bybit-fetcher = {
    description = "Bybit Data Fetcher Service";
    after = [
      "network.target"
      "clickhouse.service"
    ];
    wantedBy = [ "multi-user.target" ];

    serviceConfig = {
      ExecStart = "${self.packages.${pkgs.system}.rustPackage}/bin/bybit-data-fetcher";
      Restart = "always";
      RestartSec = 5;
      User = "root";
    };
  };
  users.users.root = {
    initialPassword = "your_password";
    openssh.authorizedKeys.keys = [
      ""
    ];
  };

  users.users.your_user = {
    # change your user
    isNormalUser = true;
    extraGroups = [ "wheel" ];
    initialPassword = "your_password";
    openssh.authorizedKeys.keys = [
      ""
    ];
  };

  security.sudo.wheelNeedsPassword = false;
  programs.zsh = {
    enable = true;
    enableCompletion = true;
    enableGlobalCompInit = false;
    promptInit = "";
  };

  services = {
    clickhouse = {
      serverConfig = {
        storage_configuration = {
          hdd = {
            path = "your_path"; # your password

          };
        };
      };
      usersConfig = {
        users = {
          default = {
            access_management = 1;
            named_collection_control = 1;
            show_named_collections = 1;
            show_named_collections_secrets = 1;
            password = "your_password"; # your password
          };

        };
      };
      enable = true;
    };
  };

  system.stateVersion = "25.05";
}
