{
  description = "Bybit Data Fetcher with NixOS Deployment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
    disko = {
      url = "github:nix-community/disko";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      flake-utils,
      disko,
      ...
    }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      eachSystemOutputs = flake-utils.lib.eachDefaultSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          rustNightly = fenix.packages.${system}.complete.withComponents [
            "rust-analyzer-preview"
            "cargo"
            "clippy-preview"
            "rustc"
            "rustfmt-preview"
            "rust-std"
            "rust-src"
          ];

          rustPlatform = pkgs.makeRustPlatform {
            cargo = rustNightly;
            rustc = rustNightly;
          };

          crate = rustPlatform.buildRustPackage {
            pname = "bybit-data-fetcher";
            version = "0.1.0";
            src = self;
            cargoLock = {
              lockFile = ./Cargo.lock;
            };
          };
        in
        {
          packages = {
            default = crate;
            rustPackage = crate;
            docker = pkgs.dockerTools.buildImage {
              name = "bybit-data-fetcher";
              config.Cmd = [ "${crate}/bin/bybit-data-fetcher" ];
            };
          };

          devShells.default = pkgs.mkShell {
            nativeBuildInputs = [
              rustNightly
              pkgs.llvmPackages.libclang
              pkgs.llvmPackages.llvm
            ];
            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
            LD_LIBRARY_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          };
        }
      );
    in
    {
      nixosConfigurations.my-server = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        specialArgs = { inherit self; };
        modules = [
          disko.nixosModules.disko
          ./disk-config.nix
          ./configuration.nix
        ];
      };
    };
}
