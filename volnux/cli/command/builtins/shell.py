from typing import Optional

from ..base import BaseCommand, CommandCategory


class ShellCommand(BaseCommand):
    help = "Start an interactive Python shell with Volnux context"
    name = "shell"
    category = CommandCategory.DEVELOPMENT

    def handle(self, *args, **options) -> Optional[str]:
        import code

        self.stdout.write("Starting Volnux interactive shell...\n")

        local_vars = {
            "volnux": "Volnux context loaded",
        }

        code.interact(local=local_vars, banner="Volnux Shell")

        return None
