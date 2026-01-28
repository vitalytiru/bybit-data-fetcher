{

  description = "Rust nightly development environment with LLVM tools";

  inputs = {

    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    fenix = {

      url = "github:nix-community/fenix";

      inputs.nixpkgs.follows = "nixpkgs";

    };

    flake-utils.url = "github:numtide/flake-utils";

  };

  outputs =

    {

      self,

      nixpkgs,

      fenix,

      flake-utils,

    }:

    flake-utils.lib.eachDefaultSystem (

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

        crate = pkgs.rustPlatform.buildRustPackage {

          pname = "bybit-data-fetcher";

          version = "0.1.0";

          src = self;

          cargoLock = {

            lockFile = ./Cargo.lock;

          };

        };

        rustPlatform = pkgs.makeRustPlatform {

          cargo = rustNightly;

          rustc = rustNightly;

        };

        dockerImage = pkgs.dockerTools.buildImage {

          name = "bybit-data-fetcher";

          config = {

            Cmd = [ "${crate}/bin/bybit_data_fetcher" ];

          };

        };

      in

      {

        packages = {

          rustPackage = crate;

          docker = dockerImage;

        };

        devShells.default = pkgs.mkShell {

          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";

          LLVM_CONFIG_PATH = "${pkgs.llvmPackages.llvm.dev}/bin/llvm-config";

          LD_LIBRARY_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";

          shellHook = ''

            export CFLAGS="-O1"

          '';

        };

        defaultPackage = dockerImage;

      }

    );

}
