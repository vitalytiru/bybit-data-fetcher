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
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            rustNightly
          ];

          # tell clang-sys where to look
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          LLVM_CONFIG_PATH = "${pkgs.llvmPackages.llvm.dev}/bin/llvm-config";

          # optional: make the *.so findable at run-time
          LD_LIBRARY_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";

          shellHook = ''
            export CFLAGS="-O1"
          '';
        };
      }
    );
}
