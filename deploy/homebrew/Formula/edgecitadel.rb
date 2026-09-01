class Edgecitadel < Formula
  desc "Create, join, and operate an EdgeCitadel agent network"
  homepage "https://github.com/zhonghaozhan/EdgeCitadel"
  license "MIT"
  head "https://github.com/zhonghaozhan/EdgeCitadel.git", branch: "main"

  depends_on "nats-server"
  depends_on "python@3.12"

  def install
    libexec.install ".env.example", "docker-compose.yml"
    libexec.install "aggregator", "frontend", "nats", "nginx"
    libexec.install "plugin-toolkit", "plugins", "schemas", "scripts"

    python = formula_opt_bin("python@3.12")/"python3.12"
    (bin/"edgecitadel").write <<~SH
      #!/bin/bash
      export EDGECITADEL_DISTRIBUTION="homebrew"
      export EDGECITADEL_INSTALL_ROOT="#{libexec}"
      exec "#{python}" "#{libexec}/scripts/edgecitadel_cli.py" "$@"
    SH
  end

  def caveats
    <<~EOS
      Edge nodes do not require Docker:
        edgecitadel join 'ecjoin://...'
        edgecitadel join 'ecjoin://...' --messaging-mode nats_leaf
        edgecitadel plugin install echo

      Core nodes require a running Docker Desktop or Docker Engine. The formula
      intentionally installs neither Docker Desktop nor a Docker daemon:
        edgecitadel create

      Persistent state is stored under ~/.edgecitadel, outside the Cellar.
      nats_leaf uses a user-level local NATS service; single-client does not
      start it.
    EOS
  end

  test do
    assert_match "edgecitadel 0.1.0", shell_output("#{bin}/edgecitadel --version")
  end
end
